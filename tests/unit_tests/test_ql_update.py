from unittest import TestCase, main

from surrealist.ql.statements.update import Update


class TestUpdate(TestCase):
    def test_update_default(self):
        show = Update(None, "person")
        self.assertEqual("UPDATE person;", show.to_str())

    def test_update_id(self):
        show = Update(None, "person", record_id="tobie")
        self.assertEqual("UPDATE person:tobie;", show.to_str())

    def test_update_default_only(self):
        update = Update(None, "person:tobie").only()
        self.assertEqual("UPDATE ONLY person:tobie;", update.to_str())

    def test_update_full(self):
        update = Update(None, "person").where("age < 18").return_none().timeout("10s").parallel()
        self.assertEqual("UPDATE person WHERE age < 18 RETURN NONE TIMEOUT 10s PARALLEL;", update.to_str())

    def test_update_returns(self):
        update = Update(None, "person").where("age < 18").return_after()
        self.assertEqual("UPDATE person WHERE age < 18 RETURN AFTER;", update.to_str())
        update = Update(None, "person").where("age < 18").return_before()
        self.assertEqual("UPDATE person WHERE age < 18 RETURN BEFORE;", update.to_str())
        update = Update(None, "person").where("age < 18").return_diff()
        self.assertEqual("UPDATE person WHERE age < 18 RETURN DIFF;", update.to_str())
        update = Update(None, "person").where("age < 18").returns("id")
        self.assertEqual("UPDATE person WHERE age < 18 RETURN id;", update.to_str())

    def test_first_example(self):
        text = 'UPDATE ONLY person:tobie SET name = "Tobie", company = "SurrealDB", skills = ["Rust", "Go", "JavaScript"];'
        upd = Update(None, "person", "tobie").only().set(name="Tobie", company="SurrealDB", skills=['Rust', 'Go', 'JavaScript'])
        self.assertEqual(text, upd.to_str())
        self.assertTrue(upd.is_valid())

    def test_plus_minus_1(self):
        text = 'UPDATE webpage:home SET click_count += 1;'
        text2 = "UPDATE person:tobie SET interests -= 'Java';"
        upd = Update(None, "webpage", "home").set("click_count += 1")
        self.assertEqual(text, upd.to_str())
        self.assertTrue(upd.is_valid())
        upd = Update(None, "person", "tobie").set("interests -= 'Java'")
        self.assertEqual(text2, upd.to_str())
        self.assertTrue(upd.is_valid())

    def test_example2(self):
        text = "UPDATE city SET population = 9541000 WHERE name = 'London';"
        upd = Update(None, "city").set(population=9541000).where("name = 'London'")
        self.assertEqual(text, upd.to_str())
        self.assertTrue(upd.is_valid())

    def test_example3(self):
        text = 'UPDATE person:tobie CONTENT {"name": "Tobie", "company": "SurrealDB", "skills": ["Rust", "Go", "JavaScript"]};'
        data = {'name': 'Tobie', 'company': 'SurrealDB', 'skills': ['Rust', 'Go', 'JavaScript']}
        upd = Update(None, "person", "tobie").content(data)
        self.assertEqual(text, upd.to_str())
        self.assertTrue(upd.is_valid())

    def test_example4(self):
        text = 'UPDATE person MERGE {"settings": {"marketing": true}};'
        data = {'settings': {'marketing': True}}
        upd = Update(None, "person").merge(data)
        self.assertEqual(text, upd.to_str())
        self.assertTrue(upd.is_valid())

    def test_example_patch(self):
        text = 'UPDATE person:tobie PATCH [{"op": "add", "path": "Engineering", "value": "true"}];'
        data = [{"op": "add","path": "Engineering","value": "true"}]
        upd = Update(None, "person", "tobie").patch(data)
        self.assertEqual(text, upd.to_str())
        self.assertTrue(upd.is_valid())

    def test_example_5(self):
        text ='UPDATE person SET important = true WHERE ->knows->person->(knows WHERE influencer = true) TIMEOUT 5s;'
        upd = Update(None, "person").set(important=True).where("->knows->person->(knows WHERE influencer = true)").timeout("5s")
        self.assertEqual(text, upd.to_str())
        self.assertTrue(upd.is_valid())


if __name__ == '__main__':
    main()
