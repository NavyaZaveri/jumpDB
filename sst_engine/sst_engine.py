import uuid

import attr
from sortedcontainers import SortedDict, SortedSet
from collections import defaultdict
from contextlib import contextmanager
import json

TOMBSTONE = str(uuid.uuid5(uuid.NAMESPACE_OID, 'TOMBSTONE')).encode('ascii')


def chain_segments(s1, s2):
    with s1.open("r"), s2.open("r"):
        while not s1.reached_eof() and not s2.reached_eof():
            if s1.peek_entry().key < s2.peek_entry().key:
                yield s1.read_entry()
            elif s1.peek_entry().key == s2.peek_entry().key:
                yield s2.read_entry()  # take the most recent entry
                s1.read_entry()
            else:
                yield s2.read_entry()
        while not s1.reached_eof():
            yield s1.read_entry()
        while not s2.reached_eof():
            yield s2.read_entry()


def merge(s1, s2, s3):
    with s3.open("w+"):
        for entry in chain_segments(s1, s2):
            s3.add_entry(entry)


class UnsortedEntries(Exception):
    def __init__(self, *args, **kwargs):
        super(UnsortedEntries, self).__init__(args, kwargs)


class Segment:
    """
    Segment represent a sorted string table (SST).
    All k-v pairs will be sorted by key, with no duplicates
    """

    def __init__(self, path):
        self.path = path
        self.fd = None
        self.previous_entry_key = None

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

    def add_entry(self, entry):
        key = entry[0]
        print(key, self.previous_entry_key)
        if self.previous_entry_key is not None and self.previous_entry_key > key:
            raise UnsortedEntries(f"Tried to insert {key}, but previous entry {self.previous_entry_key} is bigger")

        json_str = json.dumps({entry[0]: entry[1]})
        self.previous_entry_key = key
        pos = self.fd.tell()
        self.fd.write(json_str)
        self.fd.write("\n")
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


class SparseMemoryIndex:
    def __init__(self):
        self.key_to_segments = defaultdict(list)
        self.tree = SortedSet()

    def __setitem__(self, key, value):
        self.key_to_segments[key].append(value)
        self.tree.add(key)


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
    def __init__(self, max_size=1000, path=None):
        self.mem_table = MemTable(max_size)
        self.max_size = max_size
        self._key_offsets = []
        self.immutable_segments = []
        self.sparse_memory_index = {}

    def __getitem__(self, item):
        if item in self.mem_table:
            value = self.mem_table[item]
            if value == TOMBSTONE:
                raise RuntimeError("{item} was deleted.")

    def __setitem__(self, key, value):
        if self.mem_table.capacity_reached():
            self._write_to_segment()
            self.mem_table.clear()
        else:
            self.mem_table[key] = value

    def __delitem__(self, key):
        self.mem_table[key] = TOMBSTONE

    def __contains__(self, item):
        pass

    def _should_merge(self):
        pass

    def _merge(self):
        pass

    def _write_to_segment(self):
        if self.mem_table.capacity_reached():
            segment = Segment(self.new_segment_name())
            with segment.open("r+") as segment:
                for (k, v) in self.mem_table:
                    if v != TOMBSTONE:
                        segment.add_entry((k, v))
            self.immutable_segments.append(segment)

    def load_from_data(self):
        pass

    def new_segment_name(self):
        return f"{len(self.immutable_segments) + 1}.txt"


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
        for key, value in self._entries:
            yield (key, value)
