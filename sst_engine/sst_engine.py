import os
import json
import re
import tempfile
import time
import uuid
from contextlib import contextmanager
from pybloom_live import ScalableBloomFilter
import heapq
import attr
from sortedcontainers import SortedDict
from contextlib import ExitStack

TOMBSTONE = str(uuid.uuid5(uuid.NAMESPACE_OID, 'TOMBSTONE')).encode('ascii')
SEGMENT_DIR = "sst_data"


def make_new_segment(persist=False, base_path=SEGMENT_DIR):
    if persist:
        return make_persistent_segment(base_path)
    return make_temp_segment()


def chain_segments(*segments):
    """
    Makes an iterator that yields entries from the input segments. The entries are sorted by key first, then timestamp.

    **Implementation**:

    The idea is to maintain a heap parameterized on the entry key and reverse timestamp at every iteration. At
    the beginning of every iteration, we pop an element off the heap, then check if the key has been seen before.


    If it hasn't, yield the entry, and add the next entry of the segment into the heap (as long as the segment ptr
    isn't at EOF). If the key *has* been seen before, we ignore the current key as it comes from an older segment.

    :param segments: input segments to be merged
    :return: entry generator ordered by key and timestamp
    """
    with ExitStack() as stack:
        open_segments = [stack.enter_context(segment.open("r")) for segment in segments]
        heap = []
        previous_entry = None
        for segment in open_segments:
            if not segment.reached_eof():
                entry = segment.read_entry()
                key = entry.key
                heapq.heappush(heap, (key, -segment.timestamp, entry, segment))
        while heap:
            key, ts, entry, segment = heapq.heappop(heap)
            if previous_entry is not None and entry.key == previous_entry.key:
                if not segment.reached_eof():
                    next_entry = segment.read_entry()
                    next_key = next_entry.key
                    heapq.heappush(heap, (next_key, -segment.timestamp, next_entry, segment))
                continue
            yield entry
            previous_entry = entry
            if not segment.reached_eof():
                next_entry = segment.read_entry()
                next_key = next_entry.key
                heapq.heappush(heap, (next_key, -segment.timestamp, next_entry, segment))


def make_persistent_segment(base_path):
    if not os.path.exists(base_path):
        os.makedirs(base_path)
    filepath = os.path.join(base_path, str(time.time()) + ".txt")
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


class UnsortedEntries(Exception):
    def __init__(self, *args, **kwargs):
        super(UnsortedEntries, self).__init__(args, kwargs)


class Segment:
    """
    Segment represent a sorted string table (SST).
    All k-v pairs will be sorted by key, with no duplicates.
    """

    def __init__(self, path, fd=None):
        self.path = path
        self.fd = fd
        self.previous_entry_key = None
        self.size = 0
        self._timestamp = self._extract_timestamp()

    def _extract_timestamp(self):
        # extract float representing the time of segment creation
        str_timestamp = re.findall("[+-]?\d+\.\d+", self.path)[0]
        return float(str_timestamp)

    @property
    def timestamp(self):
        return self._timestamp

    def search(self, query, offset=0):
        self.fd.seek(offset)
        while not self.reached_eof():
            entry = self.read_entry()
            if entry.key == query:
                return entry
        return None

    def __len__(self):
        return self.size

    def __lt__(self, other):
        return self.timestamp < other.timestamp

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
                 path=None,
                 merge_threshold=2):
        """

        :param max_inmemory_size: maximum number of entries to hold in memory.
        :param sparse_offset: frequency of key offsets kept in memory. (Eg: if `sparse_offset=5`, one key offset is kept
         in memory for every 5 entries.)
        :param segment_size: maximum number of entries in a given segment.
        :param persist_segments: if set to false, cleans up segment files in the end. Otherwise, retains the files in disk
        :param merge_threshold: number of segment to keep in intact before merging
        :param path: absolute path of directory to scan into for pre-existing segments
        """
        self._mem_table = MemTable(max_inmemory_size)
        self.max_inmemory_size = max_inmemory_size
        self._immutable_segments = []
        self._sparse_memory_index = SortedDict()
        self.sparse_offset = sparse_offset
        self._segment_size = segment_size
        self._entries_deleted = 0
        self._bloom_filter = ScalableBloomFilter(mode=2)
        self.persist = persist_segments
        self._merge_threshold = merge_threshold
        self._base_path = None
        if path:
            self._base_path = path
            self._scan_path_for_segments(path)

    def _scan_path_for_segments(self, path):
        """
        Scans the base path for previously existing segments.
        """

        storage = []
        if os.path.exists(path):
            for entry in os.scandir(path):
                storage.append(entry.path)
        self._immutable_segments = [Segment(path) for path in sorted(storage)]
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

        # segments are ordered by time; so we just need to search all existing segments in
        # reverse order (to get the more recent entry) up to the current segment index
        for next_segment in self._immutable_segments[-1:segment_index:-1]:
            entry = search_entry_in_segment(next_segment, item, offset)
            if entry is not None:
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
            if len(self._immutable_segments) >= self._merge_threshold:
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

    def merge(self, *segments):
        merged_segments = []

        def merge_into(new_segment, chain_gen):
            count = 0
            with new_segment.open("w+"):
                for entry in chain_gen:
                    new_segment.add_entry(entry)
                    count += 1
                    if count == self._segment_size:
                        merge_into(make_new_segment(self.persist, self._base_path), chain_gen)
                        break
            if len(new_segment) >= 1:  # just in case the generator doesn't yield anything
                merged_segments.append(new_segment)

        merge_into(make_new_segment(self.persist, self._base_path), chain_segments(*segments))
        return merged_segments[::-1]

    def _write_to_segment(self):
        """
        Creates a new segment filled with the contents of the memtable, and updates sparse table.
        Should be called only when the capacity of memtable is full.

        :return: Segment with contents of the memtable
        """
        segment = make_new_segment(self.persist, self._base_path)
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
