from entsoe.files import EntsoeFileClient
from dotenv import load_dotenv
import pytest


load_dotenv()

@pytest.fixture
def client():
    yield EntsoeFileClient()


def test_single_file(client):
    files = client.list_folder('EnergyPrices_12.1.D_r3')
    assert len(files) > 0
    df = client.download_single_file('EnergyPrices_12.1.D_r3', max(files))
    assert len(df) > 0

def test_list_file(client):
    files = client.list_folder('EnergyPrices_12.1.D_r3')
    assert len(files) > 0
    df = client.download_multiple_files([
        files[max(files)]
    ])
    assert len(df) > 0