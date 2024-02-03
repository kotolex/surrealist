import logging
import random
from pathlib import Path
from string import ascii_lowercase

from src.surrealist import Surreal
from src.surrealist.clients.http_client import HttpClient

logging.getLogger("tests")
URL = "http://127.0.0.1:8000/"
WS_URL = "ws://127.0.0.1:8000/rpc"

assert HttpClient(URL).get('health').status == 200, "Start db on localhost!"

file_path = Path(__file__).parent / "import.surql"
db = Surreal(URL, 'test', 'test', ('root', 'root'), use_http=True)
with db.connect() as connection:
    connection.import_data(file_path)


def get_random_series(length: int) -> str:
    return "".join(random.choices(list(ascii_lowercase), k=length))
