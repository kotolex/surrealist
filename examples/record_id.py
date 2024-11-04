from surrealist import RecordId, get_uuid

# Please read https://github.com/kotolex/surrealist#Using_recordid
# Refer to https://surrealdb.com/docs/surrealql/datamodel/ids

# You can create a RecordId from id in full form
record_id = RecordId('person:tobie')
print(record_id.naive_id)  # person:tobie
print(record_id.id_part)  # tobie
print(record_id.table_part)  # person
print(record_id.to_prefixed_string())  # r'person:tobie'
print(f"{record_id!r}")  # RecordId('person:tobie')
# Same RecordId as above we can create with id and table
record_id = RecordId('tobie', table='person')
print(record_id.naive_id)  # person:tobie
print(record_id.id_part)  # tobie
print(record_id.table_part)  # person
print(record_id.to_prefixed_string())  # r'person:tobie'
print(f"{record_id!r}")  # RecordId('person:tobie')

# Note! An exception will be raised if you try to create a RecordId in the wrong way
# All examples below will raise an exception:
# record_id = RecordId('tobie') - no table specified
# record_id = RecordId('tobie', table='person:tobie') - table should not contain ':'
# record_id = RecordId('person:tobie', table='other') - table names are not equal

# You can generate uuid for your new RecordId
uid = get_uuid()
record_id = RecordId(uid, table='book')
print(record_id.naive_id)  # 76bc591f-5a43-4b4a-bd5b-a52f90cded8a (your uuid will be different)
# Create record id in valid format for SurrealDB
print(record_id.to_uid_string())  # book:⟨76bc591f-5a43-4b4a-bd5b-a52f90cded8a⟩
print(record_id.to_uid_string_with_backticks())  # book:`76bc591f-5a43-4b4a-bd5b-a52f90cded8a`

# And vice versa, you can get parts of record id from book:⟨76bc591f-5a43-4b4a-bd5b-a52f90cded8a⟩
record_id = RecordId('book:⟨76bc591f-5a43-4b4a-bd5b-a52f90cded8a⟩')
print(record_id.naive_id)  # book:76bc591f-5a43-4b4a-bd5b-a52f90cded8a
print(record_id.table_part)  # book
print(record_id.id_part)  # 76bc591f-5a43-4b4a-bd5b-a52f90cded8a
