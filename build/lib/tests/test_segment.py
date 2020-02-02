import pytest

from jumpDB import DB, make_new_segment


def test_segment_reads(segment):
    entries = [("0", "0"), ("hello", "world")]
    with segment.open("w") as s:
        for entry in entries:
            s.add_entry(entry)
    res = []
    with segment.open("r") as s:
        for e in s.entries():
            res.append(e.to_pair())
    assert res == entries


def test_segment_seeks(segment):
    entry = ("foo", "bar")
    with segment.open("w") as s:
        offset = s.add_entry(entry)
    with segment.open("r") as s:
        s.seek(offset)
        inserted_entry = s.read_entry().to_pair()
    assert inserted_entry == entry


def test_segment_peeks(segment):
    entries = [("foo", "bar"), ("hello", "world")]
    with segment.open("w+") as s:
        for entry in entries:
            s.add_entry(entry)
    with segment.open("r+") as s:
        first_entry = s.peek_entry()
        same_entry = s.peek_entry()
        assert same_entry == first_entry


def test_panic_when_writing_unsorted_entries(segment):
    entries = [("z", 1), ("a", 1)]
    with segment.open("w") as s:
        with pytest.raises(Exception):
            for entry in entries:
                s.add_entry(entry)


def test_simple_segment_chaining(segment):
    segment_1_entries = [("a", "1"), ("c", "3")]
    segment_2_entries = [("b", "5")]
    segment_1 = make_new_segment()
    segment_2 = make_new_segment()
    with segment_1.open("w"), segment_2.open("w"):
        for entry in segment_1_entries:
            segment_1.add_entry(entry)
        for entry in segment_2_entries:
            segment_2.add_entry(entry)

    db = DB(persist_segments=False, segment_size=3)
    merged_segments = db.merge(segment_1, segment_2)
    assert len(merged_segments) == 1
    segment_3 = merged_segments.pop()
    with segment_3.open("r") as s3:
        assert [e.to_pair() for e in s3.entries()] \
               == sorted(segment_1_entries + segment_2_entries)


def test_segment_chaining_with_duplicate_keys():
    segment_1_entries = [("a", "1"), ("c", "3")]
    segment_2_entries = [("a", "5")]
    segment_1 = make_new_segment()
    segment_2 = make_new_segment()
    with segment_1.open("w"), segment_2.open("w"):
        for entry in segment_1_entries:
            segment_1.add_entry(entry)
        for entry in segment_2_entries:
            segment_2.add_entry(entry)

    db = DB(persist_segments=False, segment_size=3)
    merged_segments = db.merge(segment_1, segment_2)
    assert len(merged_segments) == 1
    segment_3 = merged_segments.pop()
    with segment_3.open("r") as s3:
        assert [e.to_pair() for e in s3.entries()] \
               == [("a", "5"), ("c", "3")]


def test_segment_chaining_with_no_duplicate_keys():
    segment_1_entries = [("1", "a"), ("2", "n")]
    segment_2_entries = [("3", "c"), ("4", "d")]
    segment_1 = make_new_segment()
    segment_2 = make_new_segment()
    with segment_1.open("w"), segment_2.open("w"):
        for entry in segment_1_entries:
            segment_1.add_entry(entry)
        for entry in segment_2_entries:
            segment_2.add_entry(entry)
    db = DB(segment_size=2, persist_segments=False)
    merged_segments = db.merge(segment_1, segment_2)
    assert len(merged_segments) == 2
    with merged_segments[0].open("r") as s3, merged_segments[1].open("r") as s4:
        assert [e.to_pair() for e in s3.entries()] == segment_1_entries
        assert [e.to_pair() for e in s4.entries()] == segment_2_entries


def test_segment_chaining_outputs_unique_entries():
    segment_1_entries = [("1", "a"), ("2", "n")]
    segment_2_entries = [("1", "c"), ("4", "d")]
    segment_1 = make_new_segment()
    segment_2 = make_new_segment()
    with segment_1.open("w"), segment_2.open("w"):
        for entry in segment_1_entries:
            segment_1.add_entry(entry)
        for entry in segment_2_entries:
            segment_2.add_entry(entry)
    db = DB(segment_size=3, persist_segments=False)
    merged_segments = db.merge(segment_1, segment_2)
    assert len(merged_segments) == 1
    with merged_segments[0].open("r") as s3:
        assert [e.to_pair() for e in s3.entries()] == [("1", "c"), ("2", "n"), ("4", "d")]
