import pytest
import requests

@pytest.mark.parametrize("endpoint, expected",[
    ("/posts/1", 200),
    ("/posts/999", 404)
])
def test_api_parallel(endpoint, expected):
    base_url = "https://jsonplaceholder.typicode.com"
    response = requests.get(base_url + endpoint)
    print(response.json())
    assert response.status_code == expected
