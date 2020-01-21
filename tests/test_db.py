from sst_engine.sst_engine import DB
import pytest


def test_basic_db_search_without_exceeding_capacity():
    db = DB()
    db["foo"] = "bar"
    assert db["foo"] == "bar"


def test_deletion():
    db = DB()
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
    db = DB(max_inmemory_size=2, segment_size=2)
    kv_pairs = [("k" + str(i), "v" + str(i)) for i in range(5)]
    for (k, v) in kv_pairs:
        db[k] = v
    assert len(db.immutable_segments) == 2
    for (k, v) in kv_pairs:
        assert db[k] == v


def test_db_search_with_single_merged_segment():
    db = DB(max_inmemory_size=2)
    kv_pairs = [("k1", "v1"), ("k2", "v2"), ("k1", "v1_1"), ("k2", "v2_2"), ("k3", "v3")]
    for (k, v) in kv_pairs:
        db[k] = v
    assert len(db.immutable_segments) == 1
    assert db["k1"] == "v1_1"
    assert db["k2"] == "v2_2"
