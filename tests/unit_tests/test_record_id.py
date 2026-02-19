from unittest import TestCase, main

from surrealist.errors import SurrealRecordIdError
from surrealist.utils import RecordId


class TestRecordId(TestCase):
    def test_create(self):
        record_id = RecordId('person:tobie')
        self.assertEqual(record_id.naive_id, "person:tobie")
        self.assertEqual(record_id.id_part, "tobie")
        self.assertEqual(record_id.table_part, "person")
        self.assertEqual(record_id.to_prefixed_string(), "r'person:tobie'")
        self.assertEqual(record_id.to_valid_string(), "person:tobie")
        self.assertEqual(str(record_id), "RecordId('person:tobie')")

    def test_create_with_u(self):
        record_id = RecordId("article:u'55eab6a0-ffb4-4a77-ba37-fc1c1bfb7829'")
        self.assertEqual(record_id.naive_id, "article:55eab6a0-ffb4-4a77-ba37-fc1c1bfb7829")
        self.assertEqual(record_id.id_part, "55eab6a0-ffb4-4a77-ba37-fc1c1bfb7829")
        self.assertEqual(record_id.table_part, "article")
        self.assertEqual(record_id.to_prefixed_string(), "r'article:55eab6a0-ffb4-4a77-ba37-fc1c1bfb7829'")
        self.assertEqual(record_id.to_uid_string(), "article:u'55eab6a0-ffb4-4a77-ba37-fc1c1bfb7829'")
        self.assertEqual(record_id.to_valid_string(), "article:u'55eab6a0-ffb4-4a77-ba37-fc1c1bfb7829'")
        self.assertEqual(str(record_id), "RecordId('article:55eab6a0-ffb4-4a77-ba37-fc1c1bfb7829')")

    def test_create_with_string(self):
        record_id = RecordId("article:some_name1")
        self.assertEqual(record_id.naive_id, "article:some_name1")
        self.assertEqual(record_id.id_part, "some_name1")
        self.assertEqual(record_id.table_part, "article")
        self.assertEqual(record_id.to_prefixed_string(), "r'article:some_name1'")
        self.assertEqual(record_id.to_uid_string(), "article:u'some_name1'")
        self.assertEqual(record_id.to_valid_string(), "article:`some_name1`")
        self.assertEqual(str(record_id), "RecordId('article:some_name1')")

    def test_create_with_table(self):
        record_id = RecordId('tobie', table='person')
        self.assertEqual(record_id.naive_id, "person:tobie")
        self.assertEqual(record_id.id_part, "tobie")
        self.assertEqual(record_id.table_part, "person")
        self.assertEqual(record_id.to_prefixed_string(), "r'person:tobie'")
        self.assertEqual(record_id.to_valid_string(), "person:tobie")
        self.assertEqual(str(record_id), "RecordId('person:tobie')")

    def test_raise_no_colon(self):
        with self.assertRaises(SurrealRecordIdError):
            RecordId('tobie')

    def test_raise_different_tables(self):
        with self.assertRaises(SurrealRecordIdError):
            RecordId('person:tobie', table='tobie')

    def test_raise_colon_in_table_name(self):
        with self.assertRaises(SurrealRecordIdError):
            RecordId('tobie', table='tobie:tobie')

    def test_valid_form(self):
        record_id = RecordId('person:tobie')
        self.assertEqual(record_id.to_valid_string(), "person:tobie")
        record_id = RecordId('person:100')
        self.assertEqual(record_id.to_valid_string(), "person:100")
        record_id = RecordId('person:8424486b-85b3-4448-ac8d-5d51083391c7')
        self.assertEqual(record_id.to_valid_string(), "person:u'8424486b-85b3-4448-ac8d-5d51083391c7'")


if __name__ == '__main__':
    main()
