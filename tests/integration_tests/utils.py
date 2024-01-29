import random
from string import ascii_lowercase

from py_surreal.http_client import HttpClient

URL = "http://127.0.0.1:8000/"

assert HttpClient(URL).get('health').status == 200, "Start db on localhost!"


def get_random_series(length: int) -> str:
    return "".join(random.choices(list(ascii_lowercase), k=length))
