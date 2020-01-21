import json
import tempfile
import time
import uuid
from contextlib import contextmanager

import attr
from sortedcontainers import SortedDict

TOMBSTONE = str(uuid.uuid5(uuid.NAMESPACE_OID, 'TOMBSTONE')).encode('ascii')


def make_new_segment():
    fd, path = tempfile.mkstemp(prefix=str(time.time()), suffix="txt")
    return Segment(path=path, fd=fd)


def search_entry_in_segment(segment, key, offset):
    with segment.open("r"):
        entry = segment.search(key, offset)
        if entry is not None:
            return entry
    return None


def chain_segments(s1, s2):
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

    def search(self, query_entry_key, offset):
        self.fd.seek(offset)
        while not self.reached_eof():
            entry = self.read_entry()
            if entry.key == query_entry_key:
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
            raise Exception("value needs to be a string, but {value} is not")

        if self.previous_entry_key is not None and self.previous_entry_key > key:
            raise UnsortedEntries(f"Tried to insert {key}, but previous entry {self.previous_entry_key} is bigger")

        json_str = json.dumps({entry[0]: entry[1]})
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
    def __init__(self, max_inmemory_size=1000, sparse_offset=10, segment_size=10):
        """

        :param max_inmemory_size: maximum number of entries to hold in memory.
        :param sparse_offset: frequency of key offsets kept in memory. (Eg: if `sparse_offset=5`, we store one key offset
         in memory for every 5 entries.)
        :param segment_size: maximum number of entries in a given segment.
        """
        self.mem_table = MemTable(max_inmemory_size)
        self.max_inmemory_size = max_inmemory_size
        self._immutable_segments = []
        self.sparse_memory_index = SortedDict()
        self.sparse_offset = sparse_offset
        self.segment_size = segment_size

    def segment_count(self):
        return len(self._immutable_segments)

    def get(self, item):
        if item in self.mem_table:
            value = self.mem_table[item]
            if value == TOMBSTONE:
                return None
            return value
        closest_key = next(self.sparse_memory_index.irange(maximum=item, reverse=True))
        segment, offset = self.sparse_memory_index[closest_key].segment, self.sparse_memory_index[closest_key].offset
        entry = search_entry_in_segment(segment, item, offset)
        if entry is not None:
            return entry.value
        segment_index = self._immutable_segments.index(segment)
        for next_segment in self._immutable_segments[segment_index + 1:]:
            entry = search_entry_in_segment(next_segment, item, offset)
            return entry.value
        return None

    def __getitem__(self, item):
        value = self.get(item)
        if item is None:
            raise RuntimeError(f"no value found for {item}")
        return value

    def __setitem__(self, key, value):
        if self.mem_table.capacity_reached():
            segment = self._write_to_segment()
            self._immutable_segments.append(segment)
            if len(self._immutable_segments) >= 2:
                merged_segments = self.merge(*self._immutable_segments)
                self._immutable_segments = merged_segments
                self.sparse_memory_index.clear()
                count = 0
                for segment in self._immutable_segments:
                    with segment.open("r"):
                        for offset, entry in segment.offsets_and_entries():
                            if count % self.sparse_offset == 0:
                                self.sparse_memory_index[entry.key] = KeyDirEntry(offset=offset, segment=segment)
                            count += 1
            self.mem_table.clear()
            self.mem_table[key] = value
        else:
            self.mem_table[key] = value

    def __delitem__(self, key):
        self.mem_table[key] = TOMBSTONE

    def __contains__(self, item):
        return self.get(item) is not None

    def merge(self, s1, s2):
        merged_segments = []

        def merge_into(new_segment, chain_gen):
            count = 0
            with new_segment.open("w+"):
                for entry in chain_gen:
                    new_segment.add_entry(entry)
                    count += 1
                    if count == self.segment_size:
                        merge_into(make_new_segment(), chain_gen)
                        break
            if len(new_segment) >= 1:  # just in case the generator doesn't yield anything
                merged_segments.append(new_segment)

        merge_into(make_new_segment(), chain_segments(s1, s2))
        return merged_segments[::-1]

    def _write_to_segment(self):
        segment = make_new_segment()
        with segment.open("w") as segment:
            count = 0
            for (k, v) in self.mem_table:
                if v != TOMBSTONE:  # if a key was deleted, there's no need to put in the segment
                    offset = segment.add_entry((k, v))
                    if count % self.sparse_offset == 0:
                        self.sparse_memory_index[k] = KeyDirEntry(offset=offset, segment=segment)
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
