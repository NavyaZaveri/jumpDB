from sst_engine.sst_engine import DB
import pytest


def test_simple_db_search():
    db = DB(max_inmemory_size=10)
    db["foo"] = "bar"
    assert db["foo"] == "bar"


def test_deletion():
    db = DB(max_inmemory_size=10)
    db["foo"] = "bar"
    del db["foo"]
    with pytest.raises(Exception):
        _ = db["foo"]


def test_db_search_with_exceeding_capacity():
    db = DB(max_inmemory_size=2)
    db["k1"] = "v1"
    db["k2"] = "v2"
    db["k3"] = "v3"
    assert db["k1"] == "v1"
    assert db["k2"] == "v2"
    assert db["k3"] == "v3"


def test_db_search_with_multiple_segments():
    db = DB(max_inmemory_size=2, segment_size=2, sparse_offset=5)

    # all unique k-v pairs
    kv_pairs = [("k" + str(i), "v" + str(i)) for i in range(5)]
    for (k, v) in kv_pairs:
        db[k] = v
    assert db.segment_count() == 2
    for (k, v) in kv_pairs:
        assert db[k] == v


def test_db_search_with_single_merged_segment():
    db = DB(max_inmemory_size=2, segment_size=2, sparse_offset=5)
    kv_pairs = [("k1", "v1"), ("k2", "v2"), ("k1", "v1_1"), ("k2", "v2_2"), ("k3", "v3")]
    for (k, v) in kv_pairs:
        db[k] = v
    assert db.segment_count() == 1
    assert db["k1"] == "v1_1"
    assert db["k2"] == "v2_2"


def test_db_search_for_for_deleted_key():
    db = DB(max_inmemory_size=2, segment_size=2)
    db["k1"] = "v1"
    del db["k1"]
    db["k2"] = "v2"
    with pytest.raises(Exception):
        _ = db["k1"]


def test_db_contains_key():
    db = DB(max_inmemory_size=2, segment_size=2)
    db["k1"] = "v1"
    db["k2"] = "v2"
    db["k3"] = "v3"
    del db["k2"]
    assert "k1" in db
    assert "k2" not in db
