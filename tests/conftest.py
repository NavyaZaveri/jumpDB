from sst_engine.sst_engine import Segment
import pytest


@pytest.fixture()
def segment():
    return Segment("data.txt")
