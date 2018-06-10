import pytest
import pymongo

from mongaule import FakeSocketPool


@pytest.fixture
def client():
    return pymongo.MongoClient(_pool_class=FakeSocketPool)


def test_init():
    pymongo.MongoClient(_pool_class=FakeSocketPool)


def test_find(client):
    docs = client.test.test.find()
    assert [x for x in docs] == []
