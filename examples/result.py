from surrealist import SurrealResult

# here we will create Results, but in your work you will get it from SurrealDB
# please see https://github.com/kotolex/surrealist?tab=readme-ov-file#results-and-recordid
result = SurrealResult(error="Some error")
print(result.is_error())  # True
print(result.is_empty())  # False

result = SurrealResult(result={"id": "author:john", "title": "Book"})
print(result.is_error())  # False
print(result.is_empty())  # False
print(result.count())  # 1
print(result.id)  # author:john
print(result.ids)  # ['author:john'] Note: ids always return list
print(result.get("title"))  # Book
print(result.get("non-exists"))  # None, no such field
print(result.get("non-exists", 100))  # 100, no such field, but we specify default

result = SurrealResult(result=[])
print(result.is_error())  # False
print(result.is_empty())  # True, Note: empty dict, empty list, None and empty string returns True
print(result.count())  # 1
# print(result.id)  # will raise exception, cause no id in []
print(result.ids)  # [] Note: ids always return list, never raises
print(result.get("title"))  # None
print(result.get("non-exists", 100))  # 100, no such field, but we specify default

result = SurrealResult(result=[{"id": "author:john", "title": "Book"}, {"id": "author:jane", "title": "Other"}])
print(result.count())  # 2
# print(result.id)  # will raise exception, cause cant get single id from multiple records
print(result.ids)  # ['author:john', 'author:jane']
print(result.get("title"))  # None, cause get method cant handle multiple records

result = SurrealResult(result=[{"id": "author:john", "title": "Book"}, {"id": "author:jane", "title": "Other"}])
print(result.first()["id"])  # author:john
print(result.last()["id"])  # author:jane
