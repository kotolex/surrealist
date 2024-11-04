import datetime
from unittest import TestCase, main

from surrealist.utils import (to_datetime, to_surreal_datetime_str, mask_pass, clean_dates,
                              dict_to_json_str,
                              RecordId, list_to_json_str, tuple_to_json_str)


class TestUtils(TestCase):
    def test_to_datetime(self):
        self.assertEqual(to_datetime('2018-01-01T00:00:00.000000Z'), datetime.datetime(2018, 1, 1, 0, 0))
        self.assertEqual(to_datetime('d"2018-01-01T00:00:00.000000Z"'), datetime.datetime(2018, 1, 1, 0, 0))
        self.assertEqual(to_datetime("d'2018-01-01T00:00:00.000000Z'"), datetime.datetime(2018, 1, 1, 0, 0))

    def test_to_surreal_datetime_str(self):
        self.assertEqual(to_surreal_datetime_str(datetime.datetime(2018, 1, 1, 0, 0)), "d'2018-01-01T00:00:00.000000Z'")

    def test_mask_pass(self):
        self.assertEqual(mask_pass(
            "{'method': 'signin', 'params': [{'user': 'root', 'pass': '******', 'NS': 'test', 'DB': 'test'}]}"),
            "{'method': 'signin', 'params': [{'user': 'root', 'pass': '******', 'NS': 'test', 'DB': 'test'}]}")
        self.assertEqual(mask_pass('{"user":"user", "pass": "123123"}'), '{"user":"user", "pass": "******"}')

    def test_clean_dates(self):
        text = ""
        self.assertEqual(clean_dates(text), text)
        text = "2018-01-01T00:00:00.000000Z"
        self.assertEqual(clean_dates(text), text)
        text = """CREATE z CONTENT {"name": "xxx", "age": 22, "create_time": "d'2024-10-23T16:06:51.322496Z'"};"""
        expected = """CREATE z CONTENT {"name": "xxx", "age": 22, "create_time": d'2024-10-23T16:06:51.322496Z'};"""
        self.assertEqual(clean_dates(text), expected)
        text = """CREATE z CONTENT {"name": "xxx", "age": 22, "create_time": 'd"2024-10-23T16:06:51.322496Z"'};"""
        expected = """CREATE z CONTENT {"name": "xxx", "age": 22, "create_time": d"2024-10-23T16:06:51.322496Z"};"""
        self.assertEqual(clean_dates(text), expected)

    def test_dict_to_json_str(self):
        self.assertEqual(dict_to_json_str({}), "{}")
        self.assertEqual(dict_to_json_str({"a": 1}), '{"a": 1}')
        self.assertEqual(dict_to_json_str({"a": 1, "b": 2}), '{"a": 1, "b": 2}')
        self.assertEqual(dict_to_json_str({"a": 1, "b": RecordId("person:john"), "c": 3}), '{"a": 1, "c": 3, "b": person:john}')
        self.assertEqual(dict_to_json_str({"b": RecordId("person:john"), "c": 3}), '{"c": 3, "b": person:john}')
        self.assertEqual(dict_to_json_str({"b": RecordId("person:john")}), '{"b": person:john}')

    def test_list_to_json_str(self):
        self.assertEqual(list_to_json_str([]), "[]")
        self.assertEqual(list_to_json_str([1, "2"]), '[1, "2"]')
        self.assertEqual(list_to_json_str([1, RecordId("person:john")]), '[1, person:john]')
        self.assertEqual(list_to_json_str([RecordId("person:john"), 1]), '[1, person:john]')
        self.assertEqual(list_to_json_str([RecordId("person:john"), RecordId("person:tobie")]), '[person:john, person:tobie]')
        self.assertEqual(list_to_json_str([1, [RecordId("person:john"), RecordId("person:tobie")]]), '[1, [person:john, person:tobie]]')

    def test_tuple_to_json_str(self):
        self.assertEqual(tuple_to_json_str(tuple()), "()")
        self.assertEqual(tuple_to_json_str((1, None)), '(1, null)')
        self.assertEqual(tuple_to_json_str((1, RecordId("person:john"))), '(1, person:john)')
        self.assertEqual(tuple_to_json_str((RecordId("person:john"), 1)), '(1, person:john)')
        self.assertEqual(tuple_to_json_str((RecordId("person:john"), RecordId("person:tobie"))), '(person:john, person:tobie)')
        self.assertEqual(tuple_to_json_str((1, [RecordId("person:john"), RecordId("person:tobie")])), '(1, [person:john, person:tobie])')



if __name__ == '__main__':
    main()
