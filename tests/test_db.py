import os

from sst_engine import DB, make_new_segment
import pytest


def test_simple_db_search():
    db = DB(max_inmemory_size=10, persist_segments=False)
    db["foo"] = "bar"
    assert db["foo"] == "bar"


def test_deletion():
    db = DB(max_inmemory_size=10, persist_segments=False)
    db["foo"] = "bar"
    del db["foo"]
    with pytest.raises(Exception):
        _ = db["foo"]


def test_db_search_with_exceeding_capacity():
    db = DB(max_inmemory_size=2, persist_segments=False)
    db["k1"] = "v1"
    db["k2"] = "v2"
    db["k3"] = "v3"
    assert db["k1"] == "v1"
    assert db["k2"] == "v2"
    assert db["k3"] == "v3"


def test_db_search_with_multiple_segments():
    db = DB(max_inmemory_size=2, segment_size=2, sparse_offset=5, persist_segments=False)

    # all unique k-v pairs
    kv_pairs = [("k" + str(i), "v" + str(i)) for i in range(5)]
    for (k, v) in kv_pairs:
        db[k] = v

    # we'll have 2 segments, each containing 2 entries); the memtable will contain the last entry
    assert db.segment_count() == 2
    for (k, v) in kv_pairs:
        assert db[k] == v


def test_db_search_with_single_merged_segment():
    db = DB(max_inmemory_size=2, segment_size=2, sparse_offset=5, persist_segments=False)
    kv_pairs = [("k1", "v1"), ("k2", "v2"), ("k1", "v1_1"), ("k2", "v2_2"), ("k3", "v3")]
    for (k, v) in kv_pairs:
        db[k] = v
    assert db.segment_count() == 1
    assert db["k1"] == "v1_1"
    assert db["k2"] == "v2_2"


def test_db_search_for_for_deleted_key():
    db = DB(max_inmemory_size=2, segment_size=2, persist_segments=False)
    db["k1"] = "v1"
    del db["k1"]
    db["k2"] = "v2"
    with pytest.raises(Exception):
        _ = db["k1"]


def test_db_contains_key():
    db = DB(max_inmemory_size=2, segment_size=2, persist_segments=False)
    db["k1"] = "v1"
    db["k2"] = "v2"
    db["k3"] = "v3"
    del db["k2"]
    assert "k1" in db
    assert "k2" not in db


def test_db_deletion_on_nonexistent_key():
    db = DB(max_inmemory_size=2, segment_size=2, persist_segments=False)
    with pytest.raises(Exception):
        _ = db["k1"]


def test_db_segment_loading():
    segment = make_new_segment(persist=True, base_path="sst_data")
    segment_entry = ("k1", "v1")
    with segment.open("w"):
        segment.add_entry(segment_entry)
    try:
        current_test_path = os.path.abspath(os.path.join(os.getcwd(), "sst_data"))
        db = DB(path=current_test_path)
        assert db.segment_count() == 1
        assert db["k1"] == "v1"

    finally:
        os.remove(segment.path)


def test_merging_with_n_segments():
    kv_pairs = [("k1", "v1"), ("k2", "v2"), ("k3", "v3"), ("k4", "k4"), ("k5", "v5")]
    db = DB(max_inmemory_size=1, segment_size=1, merge_threshold=4, persist_segments=False)
    for (k, v) in kv_pairs:
        db[k] = v
    assert db.segment_count() == 4
    for (k, v) in kv_pairs:
        assert db[k] == v


def test_internal_segment_ordering():
    segment_1 = make_new_segment(persist=True, base_path="sst_data")
    segment_1_entry = ("k1", "v1")
    segment_2 = make_new_segment(persist=True, base_path="sst_data")
    segment_2_entry = ("k2", "v2")
    segment_3 = make_new_segment(persist=True, base_path="sst_data")
    segment_3_entry = ("k2", "v2_2")
    with segment_1.open("w"), segment_2.open("w"), segment_3.open("w"):
        segment_1.add_entry(segment_1_entry)
        segment_2.add_entry(segment_2_entry)
        segment_3.add_entry(segment_3_entry)
    try:
        current_test_path = os.path.abspath(os.path.join(os.getcwd(), "sst_data"))
        db = DB(path=current_test_path)
        assert db.segment_count() == 3
        assert db["k1"] == "v1"
        assert db["k2"] == "v2_2"

    finally:
        os.remove(segment_1.path)
        os.remove(segment_2.path)
        os.remove(segment_3.path)


def test_worst_case_get():
    """
    In this specific example, we try to find the value corresponding to "k1_1"

    With the given db parameters, the sparse index will  have only one entry: "k1" -> segment_2
    Thus, we now have to look into all all segments to find correct entry

    :return:
    """
    segment_1 = make_new_segment(persist=True, base_path="sst_data")
    segment_1_entries = [("k1", "v1"), ("k1_1", "v_1")]
    segment_2 = make_new_segment(persist=True, base_path="sst_data")
    segment_2_entries = [("k1", "v1")]
    with segment_1.open("w"), segment_2.open("w"):
        for e in segment_1_entries:
            segment_1.add_entry(e)
        for e in segment_2_entries:
            segment_2.add_entry(e)
    try:
        current_test_path = os.path.abspath(os.path.join(os.getcwd(), "sst_data"))
        db = DB(path=current_test_path, sparse_offset=2)
        assert db.segment_count() == 2
        assert db["k1_1"] == "v_1"
    finally:
        os.remove(segment_1.path)
        os.remove(segment_2.path)
