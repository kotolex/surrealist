# import logging
import random
import sys
from pathlib import Path
from string import ascii_lowercase
# from logging import DEBUG, basicConfig

TESTS = Path(__file__).parent.parent
SRC = TESTS.parent / "src"
sys.path.append(str(SRC))

from src.surrealist import Surreal
from src.surrealist.clients.http_client import HttpClient

# logging.getLogger("tests")
URL = "http://127.0.0.1:8000/"
WS_URL = "ws://127.0.0.1:8000/rpc"
# basicConfig(level=DEBUG)
assert HttpClient(URL).get('health').status == 200, "Start db on localhost!"

file_path = Path(__file__).parent / "import.surql"
db = Surreal(URL, credentials=('root', 'root'), use_http=True)
with db.connect() as connection:
    connection.use("test", "test")
    connection.import_data(file_path)


def get_random_series(length: int) -> str:
    return "".join(random.choices(list(ascii_lowercase), k=length))
