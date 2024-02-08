from unittest import TestCase, main

from surrealist.ql.delete import Delete


class TestDelete(TestCase):
    def test_delete_default(self):
        show = Delete(None, "person")
        self.assertEqual("DELETE person;", show.to_str())

    def test_delete_id(self):
        show = Delete(None, "person", record_id="tobie")
        self.assertEqual("DELETE person:tobie;", show.to_str())

    def test_delete_default_only(self):
        delete = Delete(None, "person:tobie").only()
        self.assertEqual("DELETE ONLY person:tobie;", delete.to_str())

    def test_delete_full(self):
        delete = Delete(None, "person").where("age < 18").return_none().timeout("10s").parallel()
        self.assertEqual("DELETE person WHERE age < 18 RETURN NONE TIMEOUT 10s PARALLEL;", delete.to_str())

    def test_delete_returns(self):
        delete = Delete(None, "person").where("age < 18").return_after()
        self.assertEqual("DELETE person WHERE age < 18 RETURN AFTER;", delete.to_str())
        delete = Delete(None, "person").where("age < 18").return_before()
        self.assertEqual("DELETE person WHERE age < 18 RETURN BEFORE;", delete.to_str())
        delete = Delete(None, "person").where("age < 18").return_diff()
        self.assertEqual("DELETE person WHERE age < 18 RETURN DIFF;", delete.to_str())
        delete = Delete(None, "person").where("age < 18").returns("id")
        self.assertEqual("DELETE person WHERE age < 18 RETURN id;", delete.to_str())


if __name__ == '__main__':
    main()
