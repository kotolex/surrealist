from unittest import TestCase, main

from surrealist.ql.statements.show import Show


class TestShow(TestCase):
    def test_create_default(self):
        show = Show(None, "person")
        self.assertTrue("SHOW CHANGES FOR TABLE person SINCE " in show.to_str())

    def test_create_since(self):
        show = Show(None, "person").since("2023-09-07T01:23:52Z")
        self.assertEqual('SHOW CHANGES FOR TABLE person SINCE d"2023-09-07T01:23:52Z";', show.to_str())
        self.assertTrue(show.is_valid())
        show = Show(None, "person").since("2024-02-06T10:48:08.700483Z")
        self.assertEqual('SHOW CHANGES FOR TABLE person SINCE d"2024-02-06T10:48:08.700483Z";', show.to_str())
        self.assertTrue(show.is_valid())

    def test_invalid_since(self):
        show = Show(None, "person").since("2023-09-07")
        self.assertEqual('SHOW CHANGES FOR TABLE person SINCE d"2023-09-07";', show.to_str())
        self.assertFalse(show.is_valid())

    def test_full_query(self):
        show = Show(None, "person").since("2023-09-07T01:23:52Z").limit(10)
        self.assertEqual('SHOW CHANGES FOR TABLE person SINCE d"2023-09-07T01:23:52Z" LIMIT 10;', show.to_str())

    def test_invalid_full_query(self):
        show = Show(None, "person").since("2023-09-07T01:23:52Z").limit(-1)
        self.assertEqual('SHOW CHANGES FOR TABLE person SINCE d"2023-09-07T01:23:52Z" LIMIT -1;', show.to_str())
        self.assertFalse(show.is_valid())

    def test_full_invalid(self):
        show = Show(None, "person").since("2023-09-07").limit(-1)
        self.assertFalse(show.is_valid())
        self.assertEqual(['Timestamp in the wrong format, you need iso-date like 2024-01-01T10:10:10.000001Z',
                          "Limit should not be less than 1"], show.validate())


if __name__ == '__main__':
    main()
