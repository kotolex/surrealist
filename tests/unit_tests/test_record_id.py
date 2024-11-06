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
        self.assertEqual(record_id.to_uid_string(), "person:⟨tobie⟩")
        self.assertEqual(record_id.to_uid_string_with_backticks(), "person:`tobie`")
        self.assertEqual(str(record_id), "RecordId('person:tobie')")

    def test_create_with_table(self):
        record_id = RecordId('tobie', table='person')
        self.assertEqual(record_id.naive_id, "person:tobie")
        self.assertEqual(record_id.id_part, "tobie")
        self.assertEqual(record_id.table_part, "person")
        self.assertEqual(record_id.to_prefixed_string(), "r'person:tobie'")
        self.assertEqual(record_id.to_uid_string(), "person:⟨tobie⟩")
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
        self.assertEqual(record_id.to_valid_string(), "person:⟨8424486b-85b3-4448-ac8d-5d51083391c7⟩")


if __name__ == '__main__':
    main()
