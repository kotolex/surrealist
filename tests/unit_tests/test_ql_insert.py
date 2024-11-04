from unittest import TestCase, main

from surrealist import RecordId
from surrealist.ql.statements.insert import Insert
from surrealist.ql.statements.select import Select


class TestInsert(TestCase):
    def test_insert_default(self):
        data = {
            'name': 'SurrealDB',
            'founded': "2021-09-10",
            'founders': ['person:tobie', 'person:jaime'],
            'tags': ['big data', 'database']
        }
        insert = Insert(None, "person", data)
        self.assertEqual('INSERT INTO person {"name": "SurrealDB", "founded": "2021-09-10", "founders": '
                         '["person:tobie", "person:jaime"], "tags": ["big data", "database"]};', insert.to_str())
        self.assertTrue(insert.is_valid())

    def test_insert_with_record_id(self):
        data = {
            'name': 'SurrealDB',
            'founded': RecordId("2021-09-10", "dates"),
        }
        insert = Insert(None, "person", data)
        self.assertEqual('INSERT INTO person {"name": "SurrealDB", "founded": dates:⟨2021-09-10⟩};', insert.to_str())
        self.assertTrue(insert.is_valid())

    def test_failed_no_args(self):
        with self.assertRaises(ValueError):
            Insert(None, "person")

    def test_tuple_with_record_id_values(self):
        text = 'INSERT INTO some (url, link) VALUES ("salesforce.com", vanse:⟨4ccfa3ce-fc31-4cd8-a239-384c26ec95c8⟩);'
        record_id = RecordId('4ccfa3ce-fc31-4cd8-a239-384c26ec95c8', table='vanse')
        record_id2 = RecordId('jhonny', table='vanse')
        insert = Insert(None, "some", ("url", "link"), ('salesforce.com', record_id))
        self.assertEqual(text, insert.to_str())
        self.assertTrue(insert.is_valid())
        text = 'INSERT INTO some (url, link) VALUES ("salesforce.com", vanse:⟨4ccfa3ce-fc31-4cd8-a239-384c26ec95c8⟩), ("other.com", vanse:jhonny);'
        insert = Insert(None, "some", ("url", "link"), ('salesforce.com', record_id), ('other.com', record_id2))
        self.assertEqual(text, insert.to_str())
        self.assertTrue(insert.is_valid())

    def test_simple_values(self):
        text = 'INSERT INTO company (name, founded) VALUES ("SurrealDB", "2021-09-10");'
        insert = Insert(None, "company", ("name", "founded"), ('SurrealDB', '2021-09-10'))
        self.assertEqual(text, insert.to_str())
        self.assertTrue(insert.is_valid())

    def test_more_values(self):
        text = 'INSERT INTO company (name, founded) VALUES ("Acme Inc.", "1967-05-03"), ("Apple Inc.", "1976-04-01");'
        insert = Insert(None, "company", ("name", "founded"), ('Acme Inc.', '1967-05-03'),
                        ('Apple Inc.', '1976-04-01'))
        self.assertEqual(text, insert.to_str())
        self.assertTrue(insert.is_valid())

    def test_duplicate(self):
        text = 'INSERT INTO product (name, url) VALUES ("Salesforce", "salesforce.com") ON DUPLICATE KEY UPDATE tags += \'crm\';'
        insert = Insert(None, "product", ("name", "url"), ('Salesforce', 'salesforce.com')).on_duplicate(
            "tags += 'crm'")
        self.assertEqual(text, insert.to_str())
        self.assertTrue(insert.is_valid())

    def test_bulk(self):
        text = 'INSERT INTO person [{"id": "person:jaime", "name": "Jaime", "surname": "Morgan Hitchcock"}, ' \
               '{"id": "person:tobie", "name": "Tobie", "surname": "Morgan Hitchcock"}];'
        data=  [{'id': "person:jaime", 'name': "Jaime", 'surname': "Morgan Hitchcock"},
                {'id': "person:tobie", 'name': "Tobie", 'surname': "Morgan Hitchcock"} ]
        insert = Insert(None, "person", data)
        self.assertEqual(text, insert.to_str())
        self.assertTrue(insert.is_valid())

    def test_bulk_with_records(self):
        text = 'INSERT INTO person [{"name": "Jaime", "id": person:jaime}, {"name": "Tobie", "id": person:tobie}];'
        data = [{'id': RecordId("person:jaime"), 'name': "Jaime"},
                {'id': RecordId("person:tobie"), 'name': "Tobie"}]
        insert = Insert(None, "person", data)
        self.assertEqual(text, insert.to_str())
        self.assertTrue(insert.is_valid())

    def test_with_statement(self):
        text = "INSERT INTO recordings_san_francisco (SELECT * FROM temperature WHERE city = 'San Francisco');"
        insert = Insert(None, "recordings_san_francisco", Select(None, "temperature").where("city = 'San Francisco'"))
        self.assertEqual(text, insert.to_str())
        self.assertTrue(insert.is_valid())


if __name__ == '__main__':
    main()
