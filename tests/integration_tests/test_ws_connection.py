from pathlib import Path
from unittest import TestCase, main

from py_surreal.surreal import Surreal
from tests.integration_tests.utils import URL, get_uuid


class TestWebSocketConnection(TestCase):
    def test_connect(self):
        surreal = Surreal(URL, namespace="test", database="test", credentials=('root', 'root'))
        with surreal.connect() as connection:
            self.assertTrue(connection.is_connected())
            self.assertEqual("ws://127.0.0.1:8000/rpc", connection._base_url)

    def test_use(self):
        surreal = Surreal(URL, credentials=('root', 'root'))
        with surreal.connect() as connection:
            res = connection.use('test', 'test')
            self.assertFalse(res.is_error(), res)
            self.assertEqual(None, res.result)

    def test_info(self):
        surreal = Surreal(URL, namespace="test", database="test", credentials=('root', 'root'))
        with surreal.connect() as connection:
            res = connection.info()
            self.assertFalse(res.is_error(), res)
            self.assertEqual(None, res.result)



if __name__ == '__main__':
    main()