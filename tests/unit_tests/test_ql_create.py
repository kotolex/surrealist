from unittest import TestCase, main

from surrealist.ql.create import Create


class TestCreate(TestCase):
    def test_create_default(self):
        create = Create(None, "person")
        self.assertEqual("CREATE person;", create.to_str())

    def test_create_default_only(self):
        create = Create(None, "person").only()
        self.assertEqual("CREATE ONLY person;", create.to_str())

    def test_create_default_only_id(self):
        create = Create(None, "person", "tobie").only()
        self.assertEqual("CREATE ONLY person:tobie;", create.to_str())

    def test_example_1(self):
        # https://docs.surrealdb.com/docs/1.2.x/surrealql/statements/create#example-usage
        self.assertEqual(
            'CREATE person SET name = "Tobie", company = "SurrealDB", skills = ["Rust", "Go", "JavaScript"];',
            Create(None, "person").set(name="Tobie", company="SurrealDB", skills=["Rust", "Go", "JavaScript"]).to_str())
        self.assertEqual(
            'CREATE ONLY person:tobie SET name = "Tobie", company = "SurrealDB", skills = ["Rust", "Go", "JavaScript"];',
            Create(None, "person:tobie").only().set(name="Tobie", company="SurrealDB",
                                                    skills=["Rust", "Go", "JavaScript"]).to_str())
        self.assertEqual(
            'CREATE person:tobie CONTENT {"name": "Tobie", "company": "SurrealDB", "skills": ["Rust"]};',
            Create(None, "person:tobie").content(
                {"name": "Tobie", "company": "SurrealDB", "skills": ["Rust"]}).to_str())

    def test_returns(self):
        self.assertEqual('CREATE person SET age = 46, username = "john-smith" RETURN NONE;',
                         Create(None, "person").set(age=46, username="john-smith").return_none().to_str())
        self.assertEqual('CREATE person SET age = 46, username = "john-smith" RETURN DIFF;',
                         Create(None, "person").set(age=46, username="john-smith").return_diff().to_str())
        self.assertEqual('CREATE person SET age = 46, username = "john-smith" RETURN BEFORE;',
                         Create(None, "person").set(age=46, username="john-smith").return_before().to_str())
        self.assertEqual('CREATE person SET age = 46, username = "john-smith" RETURN AFTER;',
                         Create(None, "person").set(age=46, username="john-smith").return_after().to_str())
        self.assertEqual('CREATE person SET age = 46, username = "john-smith" RETURN interests;',
                         Create(None, "person").set(age=46, username="john-smith").returns("interests").to_str())


if __name__ == "__main__":
    main()
