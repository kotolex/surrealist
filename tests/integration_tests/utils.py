import uuid

from py_surreal.http_client import HttpClient

URL = "http://127.0.0.1:8000/"

assert HttpClient(URL).get('').status == 200, "Start db on localhost!"

def get_uuid():
    return str(uuid.uuid4())