import random
from pathlib import Path
from string import ascii_lowercase

from py_surreal import Surreal
from py_surreal.clients.http_client import HttpClient

URL = "http://127.0.0.1:8000/"

assert HttpClient(URL).get('health').status == 200, "Start db on localhost!"

file_path = Path(__file__).parent / "import.surql"
db = Surreal(URL, 'test', 'test', ('root', 'root'), use_http=True)
with db.connect() as connection:
    res = connection.import_data(file_path)
    print("="*15)
    print(res)
    print("=" * 15)


def get_random_series(length: int) -> str:
    return "".join(random.choices(list(ascii_lowercase), k=length))
