import os
import json
import re
import tempfile
import time
import uuid
from contextlib import contextmanager
from pybloom_live import ScalableBloomFilter

import attr
from sortedcontainers import SortedDict

TOMBSTONE = str(uuid.uuid5(uuid.NAMESPACE_OID, 'TOMBSTONE')).encode('ascii')
DATA_FILE_PATH = "sst_data"


def make_new_segment(persist=False):
    if persist:
        return make_persistent_segment()
    return make_temp_segment()


def make_persistent_segment():
    if not os.path.exists(DATA_FILE_PATH):
        os.makedirs(DATA_FILE_PATH)
    filepath = os.path.join(DATA_FILE_PATH, str(time.time()) + ".txt")
    return Segment(filepath)


def make_temp_segment():
    fd, path = tempfile.mkstemp(prefix=str(time.time()), suffix="txt")
    return Segment(path=path, fd=fd)


def search_entry_in_segment(segment, key, offset):
    with segment.open("r"):
        entry = segment.search(key, offset)
        if entry is not None:
            return entry
    return None


def chain_segments(s1, s2):
    """
    Chains entries of two segments into one, ensuring that the result is sorted
    by key and time

    :param s1: old segment
    :param s2: more recent segment
    """
    with s1.open("r"), s2.open("r"):
        while not s1.reached_eof() and not s2.reached_eof():
            if s1.peek_entry().key < s2.peek_entry().key:
                yield s1.read_entry()
            elif s1.peek_entry().key == s2.peek_entry().key:
                yield s2.read_entry()  # segment_2 was produced after segmented_i, so we take the more recent entry
                s1.read_entry()
            else:
                yield s2.read_entry()
        while not s1.reached_eof():
            yield s1.read_entry()
        while not s2.reached_eof():
            yield s2.read_entry()


class UnsortedEntries(Exception):
    def __init__(self, *args, **kwargs):
        super(UnsortedEntries, self).__init__(args, kwargs)


class Segment:
    """
    Segment represent a sorted string table (SST).
    All k-v pairs will be sorted by key, with no duplicates
    """

    def __init__(self, path, fd=None):
        self.path = path
        self.fd = fd
        self.previous_entry_key = None
        self.size = 0

    def search(self, query, offset=0):
        self.fd.seek(offset)
        while not self.reached_eof():
            entry = self.read_entry()
            if entry.key == query:
                return entry
        return None

    def __len__(self):
        return self.size

    def reached_eof(self):
        cur_pos = self.fd.tell()
        maybe_entry = self.fd.readline()
        self.fd.seek(cur_pos)
        return maybe_entry == ""

    def peek_entry(self):
        cur_pos = self.fd.tell()
        entry = self.read_entry()
        self.fd.seek(cur_pos)
        return entry

    def entries(self):
        while not self.reached_eof():
            entry = self.fd.readline()
            yield SegmentEntry.from_dict(json.loads(entry))

    def offsets_and_entries(self):
        while not self.reached_eof():
            offset = self.fd.tell()
            entry = self.fd.readline()
            yield offset, SegmentEntry.from_dict(json.loads(entry))

    def add_entry(self, entry):
        key = entry[0]
        value = entry[1]
        if not isinstance(value, str):
            raise Exception(f"value needs to be a string, but {value} is not")

        if self.previous_entry_key is not None and self.previous_entry_key > key:
            raise UnsortedEntries(f"Tried to insert {key}, but previous entry {self.previous_entry_key} is bigger")

        json_str = json.dumps({key: value})
        self.previous_entry_key = key
        pos = self.fd.tell()
        self.fd.write(json_str)
        self.fd.write("\n")
        self.size += 1
        return pos

    def read_entry(self):
        entry_dict = json.loads(self.fd.readline())
        return SegmentEntry.from_dict(entry_dict)

    def seek(self, pos):
        self.fd.seek(pos)

    @contextmanager
    def open(self, mode):
        try:
            self.fd = open(self.path, mode)
            yield self
        finally:
            self.fd.close()
            self.previous_entry_key = None


@attr.s(frozen=True)
class KeyDirEntry:
    offset = attr.ib()
    segment = attr.ib()


@attr.s(frozen=True)
class SegmentEntry:
    key = attr.ib()
    value = attr.ib()

    @classmethod
    def from_dict(cls, d):
        key, value = d.popitem()
        return cls(key, value)

    @classmethod
    def from_pair(cls, pair):
        key, value = pair
        return cls(key, value)

    def to_dict(self):
        return {self.key: self.value}

    def to_pair(self):
        return self.key, self.value

    def __getitem__(self, item):
        if item == 0:
            return self.key
        elif item == 1:
            return self.value
        raise Exception("SegmentEntry can be indexed only by 0 (key) or 1 (value)")


class DB:
    def __init__(self, max_inmemory_size=1000, sparse_offset=100, segment_size=500,
                 persist_segments=True,
                 load_from_path=False):
        """

        :param max_inmemory_size: maximum number of entries to hold in memory.
        :param sparse_offset: frequency of key offsets kept in memory. (Eg: if `sparse_offset=5`, one key offset is kept
         in memory for every 5 entries.)
        :param segment_size: maximum number of entries in a given segment.
        """
        self._mem_table = MemTable(max_inmemory_size)
        self.max_inmemory_size = max_inmemory_size
        self._immutable_segments = []
        self._sparse_memory_index = SortedDict()
        self.sparse_offset = sparse_offset
        self.segment_size = segment_size
        self._entries_deleted = 0
        self._bloom_filter = ScalableBloomFilter(mode=2)
        self.persist = persist_segments
        if load_from_path:
            self._scan_path_for_segments()

    def _scan_path_for_segments(self):

        def segment_file_cmp(filename):

            # extract float representing the time of segment creation
            file_number = re.findall("[+-]?\d+\.\d+", filename)[0]
            return float(file_number)

        storage = []
        if os.path.exists(DATA_FILE_PATH):
            for entry in os.scandir(DATA_FILE_PATH):
                storage.append(entry.path)
        self._immutable_segments = [Segment(path) for path in sorted(storage, key=segment_file_cmp)]
        count = 0
        for segment in self._immutable_segments:
            with segment.open("r"):
                for offset, entry in segment.offsets_and_entries():
                    if count % self.sparse_offset == 0:
                        self._sparse_memory_index[entry.key] = KeyDirEntry(offset=offset, segment=segment)
                    count += 1

    def segment_count(self):
        return len(self._immutable_segments)

    def get(self, item):
        if item in self._mem_table:
            value = self._mem_table[item]
            if value == TOMBSTONE:
                return None
            return value
        if len(self._sparse_memory_index) == 0:
            return None
        closest_key = next(self._sparse_memory_index.irange(maximum=item, reverse=True))
        segment, offset = self._sparse_memory_index[closest_key].segment, self._sparse_memory_index[closest_key].offset
        entry = search_entry_in_segment(segment, item, offset)
        if entry is not None:
            return entry.value
        segment_index = self._immutable_segments.index(segment)
        for next_segment in self._immutable_segments[segment_index + 1:]:
            entry = search_entry_in_segment(next_segment, item, offset)
            return entry.value
        return None

    def insert(self, key, value):
        self[key] = value

    def __getitem__(self, item):
        value = self.get(item)
        if value is None:
            raise RuntimeError(f"no value found for {item}")
        return value

    def __setitem__(self, key, value):
        if not isinstance(key, str):
            raise Exception(f"keys can only be strings; {key} is not.")
        if not isinstance(value, str):
            raise Exception(f"values can only be strings; {value} is not.")

        self._bloom_filter.add(key)
        if self._mem_table.capacity_reached():
            segment = self._write_to_segment()
            self._immutable_segments.append(segment)
            if len(self._immutable_segments) >= 2:
                merged_segments = self.merge(*self._immutable_segments)
                self._immutable_segments = merged_segments
                self._sparse_memory_index.clear()
                count = 0
                for segment in self._immutable_segments:
                    with segment.open("r"):
                        for offset, entry in segment.offsets_and_entries():
                            if count % self.sparse_offset == 0:
                                self._sparse_memory_index[entry.key] = KeyDirEntry(offset=offset, segment=segment)
                            count += 1
            self._mem_table.clear()
            self._entries_deleted = 0
            self._mem_table[key] = value
        else:
            self._mem_table[key] = value

    def __delitem__(self, key):
        if key in self:
            self._mem_table[key] = TOMBSTONE
            self._entries_deleted += 1
        else:
            raise Exception(f"{key} does not exist in the db; thus, cannot delete")

    def __contains__(self, item):
        if item not in self._bloom_filter:
            return False
        return self.get(item) is not None

    def inmemory_size(self):
        return len(self._mem_table) - self._entries_deleted

    def merge(self, s1, s2):
        merged_segments = []

        def merge_into(new_segment, chain_gen):
            count = 0
            with new_segment.open("w+"):
                for entry in chain_gen:
                    new_segment.add_entry(entry)
                    count += 1
                    if count == self.segment_size:
                        merge_into(make_new_segment(self.persist), chain_gen)
                        break
            if len(new_segment) >= 1:  # just in case the generator doesn't yield anything
                merged_segments.append(new_segment)

        merge_into(make_new_segment(self.persist), chain_segments(s1, s2))
        return merged_segments[::-1]

    def _write_to_segment(self):
        """
        Creates a new segment filled with the contents of the memtable.
        Should be called only when the capacity of memtable is full.

        :return: Segment with contents of the memtable
        """
        segment = make_new_segment(self.persist)
        with segment.open("w") as segment:
            count = 0
            for (k, v) in self._mem_table:
                if v != TOMBSTONE:  # if a key was deleted, there's no need to put in the segment
                    offset = segment.add_entry((k, v))
                    if count % self.sparse_offset == 0:
                        self._sparse_memory_index[k] = KeyDirEntry(offset=offset, segment=segment)
                    count += 1

        return segment

    def load_from_data(self):
        pass


class MemTable:
    def __init__(self, max_size):
        self._entries = SortedDict()
        self.max_size = max_size

    def __setitem__(self, key, value):
        self._entries[key] = value

    def __len__(self):
        return len(self._entries)

    def __getitem__(self, item):
        return self._entries[item]

    def clear(self):
        self._entries.clear()

    def __contains__(self, item):
        return item in self._entries

    def capacity_reached(self):
        return len(self._entries) >= self.max_size

    def __iter__(self):
        for key, value in self._entries.items():
            yield (key, value)
