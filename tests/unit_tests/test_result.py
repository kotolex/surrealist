from unittest import TestCase, main

from surrealist.result import SurrealResult, to_result

params = (
    (
        "SurrealResult(id=None, status=ERR, result=IAM error: Not enough permissions to perform this action, query=None, code=403, time=None, additional_info={'details': 'Forbidden', 'description': 'Not allowed to do this.'})",
        {'code': 403, 'details': 'Forbidden', 'description': 'Not allowed to do this.',
         'information': 'There was a problem with the database: IAM error: Not enough permissions to perform this action'}),
    ("SurrealResult(id=None, status=OK, result=None, query=None, code=None, time=67.375µs, additional_info={})",
     {'result': None, 'status': 'OK', 'time': '67.375µs'}),
    (
        "SurrealResult(id=None, status=OK, result=[{'id': 'violet:bsrx1nzm3053jlr3hxvs'}], query=None, code=None, time=3.208µs, additional_info={})",
        {'result': [{'id': 'violet:bsrx1nzm3053jlr3hxvs'}], 'status': 'OK', 'time': '3.208µs'}),
    (
        "SurrealResult(id=None, status=OK, result=[{'created_at': '2024-01-24T11:31:06.347880800Z', 'id': 'account:j716n5mprfrl11mp48mr', 'name': 'ACME Inc'}], query=None, code=None, time=32.375µs, additional_info={})",
        {'result': [
            {'created_at': '2024-01-24T11:31:06.347880800Z', 'id': 'account:j716n5mprfrl11mp48mr', 'name': 'ACME Inc'}],
            'status': 'OK', 'time': '32.375µs'}),
)

init = (
    ({'ws_id': None, 'status': 'OK', 'code': None, 'result': None, 'time': None, 'query': None, 'additional_info': {}},
     {}),
    ({'ws_id': None, 'status': 'OK', 'code': None, 'result': None, 'time': None, 'query': None,
      'additional_info': {'level': 10}}, {'level': 10}),
    ({'ws_id': None, 'status': 'ERR', 'code': 400, 'result': None, 'time': None, 'query': None, 'additional_info': {}},
     {'code': 400}),
    (
        {'ws_id': None, 'status': 'OK', 'code': None, 'result': 'text', 'time': None, 'query': None,
         'additional_info': {}},
        {'information': 'text'}),
    ({'ws_id': None, 'status': 'ERR', 'code': None, 'result': 'text', 'time': None, 'query': None,
      'additional_info': {}}, {'error': 'text'}),
    ({'ws_id': None, 'status': 'ERR', 'code': None, 'result': 'some', 'time': None, 'query': None,
      'additional_info': {}}, {'error': 'There was a problem with the database: some'}),
    ({'ws_id': None, 'status': 'ERR', 'code': None, 'result': 'some: two', 'time': None, 'query': None,
      'additional_info': {}}, {'error': 'There was a problem with the database: some: two'}),
    (
        {'ws_id': None, 'status': 'ERR', 'code': None, 'result': 'new', 'time': None, 'query': None,
         'additional_info': {}},
        {'information': 'text', 'error': 'new'}),
    ({'ws_id': None, 'status': 'ERR', 'code': None, 'result': 'token', 'time': None, 'query': None,
      'additional_info': {}}, {'information': 'text', 'error': 'new', 'token': 'token'}),
)


class TestResult(TestCase):
    def test_out(self):
        for expected, in_ in params:
            with self.subTest(f"out result on {in_}"):
                res = to_result(in_)
                self.assertEqual(expected, str(res))

    def test_init(self):
        for exp, in_ in init:
            with self.subTest(f"Init with {in_}"):
                res = SurrealResult(**in_)
                self.assertEqual(exp, res.to_dict())

    def test_is_error(self):
        self.assertTrue(SurrealResult(error="error").is_error())
        self.assertFalse(SurrealResult(result="error").is_error())
        self.assertTrue(SurrealResult(code=400).is_error())

    def test_to_result_str(self):
        res = to_result('{"id": 123, "status": "ERR", "result":"token", "time":"12s"}')
        self.assertEqual(res.to_dict(),
                         {'ws_id': 123, 'status': 'ERR', 'code': None, 'result': 'token', 'time': "12s", 'query': None,
                          'additional_info': {}})

    def test_to_result_list(self):
        res = to_result([{'code': 400}])
        self.assertEqual(res.to_dict(),
                         {'ws_id': None, 'status': 'ERR', 'code': 400, 'result': None, 'time': None, 'query': None,
                          'additional_info': {}})

    def test_to_result_list_2(self):
        res = to_result([{'code': 400}, {'information': 'text'}])
        self.assertEqual(res, SurrealResult(result=[SurrealResult(code=400), SurrealResult(information='text')]))

    def test_count(self):
        params = (
            (0, []),
            (0, None),
            (1, 0),
            (1, False),
            (1, True),
            (2, [1, 2]),
            (0, {}),
            (1, {"1": 1}),
            (1, 10),
            (1, "text"),
            (0, ""),
        )
        for expected, in_ in params:
            with self.subTest(f"count on {in_}"):
                self.assertEqual(expected, SurrealResult(result=in_).count())

    def test_id(self):
        self.assertEqual("bsrx1nzm3053jlr3hxvs", SurrealResult(result=[{'id': 'bsrx1nzm3053jlr3hxvs'}]).id)
        self.assertEqual("bsrx1nzm3053jlr3hxvs", SurrealResult(result={'id': 'bsrx1nzm3053jlr3hxvs'}).id)

    def test_id_failed_on_empty(self):
        params = ([], None, {}, '')
        for in_ in params:
            with self.subTest(f"failed on {in_}"):
                with self.assertRaises(ValueError):
                    SurrealResult(result=in_).id

    def test_id_failed_on_no_id_or_not_dict_list(self):
        params = ([1, 2], "1231242", {'1': 1}, 1234, [123], [{'1': 1}])
        for in_ in params:
            with self.subTest(f"failed on {in_}"):
                with self.assertRaises(ValueError):
                    SurrealResult(result=in_).id

    def test_ids(self):
        params = (
            ([], None),
            ([], 0),
            ([], 123),
            ([], False),
            ([], True),
            ([], {}),
            ([], []),
            ([None, None, None], [1, 2, 3]),
            ([123], {'id': 123}),
            ([123], [{'id': 123}]),
            ([123, 456], [{'id': 123}, {'id': 456}]),
            ([123, None, 456], [{'id': 123}, {"1": 1}, {'id': 456}]),
        )
        for expected, in_ in params:
            with self.subTest(f"count on {in_}"):
                self.assertEqual(expected, SurrealResult(result=in_).ids)

    def test_get(self):
        self.assertEqual(SurrealResult(result={"a": 1}).get("a"), 1)
        self.assertEqual(SurrealResult(result={"a": 1}).get("b"), None)
        self.assertEqual(SurrealResult(result=[{"a": 1}]).get("a"), 1)
        self.assertEqual(SurrealResult(result=[{"a": 1}]).get("b"), None)
        self.assertEqual(SurrealResult(result=[{"a": 1}, {"a": 2}]).get("a"), None)
        self.assertEqual(SurrealResult(result="token").get("a"), None)


if __name__ == '__main__':
    main()
