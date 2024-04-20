import datetime
from unittest import TestCase, main

from surrealist.utils import to_datetime, to_surreal_datetime_str, crop_data, mask_pass


class TestUtils(TestCase):
    def test_to_datetime(self):
        self.assertEqual(to_datetime('2018-01-01T00:00:00.000000Z'), datetime.datetime(2018, 1, 1, 0, 0))

    def test_to_surreal_datetime_str(self):
        self.assertEqual(to_surreal_datetime_str(datetime.datetime(2018, 1, 1, 0, 0)), '2018-01-01T00:00:00.000000Z')

    def test_crop_same(self):
        self.assertEqual(crop_data("one"), "one")

    def test_crop_data(self):
        self.assertEqual(crop_data("*" * 400), "*" * 300 + "...")

    def test_mask_pass(self):
        self.assertEqual(mask_pass(
            "{'method': 'signin', 'params': [{'user': 'root', 'pass': '******', 'NS': 'test', 'DB': 'test'}]}"),
            "{'method': 'signin', 'params': [{'user': 'root', 'pass': '******', 'NS': 'test', 'DB': 'test'}]}")
        self.assertEqual(mask_pass('{"user":"user", "pass": "123123"}'), '{"user":"user", "pass": "******"}')


if __name__ == '__main__':
    main()
