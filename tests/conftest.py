import pytest
from relay.store import Store


@pytest.fixture
def store(tmp_path):
    result = Store(tmp_path / "relay.db")
    result.initialize()
    return result


@pytest.fixture
def project(store):
    return store.create_project("Test project")


@pytest.fixture
def endpoint(store, project):
    return store.create_endpoint(project["id"], "https://example.com/webhook", rate_limit=1000)
