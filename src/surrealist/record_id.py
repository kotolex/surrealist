from string import ascii_lowercase, digits
from typing import Optional

from surrealist.errors import SurrealRecordIdError

ALPHABET = ascii_lowercase + digits


class RecordId:
    """
    This class is a wrapper for record_id of SurrealDB

    About record_ids: https://surrealdb.com/docs/surrealql/datamodel/ids
    Refer to: https://github.com/kotolex/surrealist?tab=readme-ov-file#using-recordid
    Examples: https://github.com/kotolex/surrealist/blob/master/examples/record_id.py
    """

    def __init__(self, id_: str, table: Optional[str] = None):
        """
        Wrapper for record_id
        :param id_: full record_id like "table:id" or just "id" part of it. In the latter case, the table should be
        specified
        :param table: name of table, you can omit it if id_ is in full form (table:id)
        :raises: SurrealRecordIdError on invalid id or table
        """
        if table is None and ":" not in id_:
            raise SurrealRecordIdError("You need to specify table name or id like 'table:id'")
        if table and ":" in table:
            raise SurrealRecordIdError("Table name should not contain ':'")
        if table and ":" in id_ and table != id_.split(":")[0]:
            table_part = id_.split(":")[0]
            raise SurrealRecordIdError(f"Table name is different from id, we expect {table}, but got {table_part}")
        id_ = id_.replace("`", "").replace("⟨", "").replace("⟩", "")
        id_ = id_.replace("u'","").replace('u"','').replace("'", "").replace('"', "")
        self._naive_id = id_ if ":" in id_ else f"{table}:{id_}"
        self._table_part, self._id_part = self._naive_id.split(":")
        self._uid = f"{self._table_part}:u'{self._id_part}'"
        self._string = f"{self._table_part}:`{self._id_part}`"

    def __repr__(self):
        return f"RecordId('{self._naive_id}')"

    @property
    def id_part(self) -> str:
        """
        Returns id part of record_id, for "table:id" it returns "id"
        """
        return self._id_part

    @property
    def table_part(self) -> str:
        """
        Returns table part of record_id, for "table:id" it returns "table"
        """
        return self._table_part

    @property
    def naive_id(self) -> str:
        """
        Returns naive record_id, without any special braces, e.g. 'table:id'
        """
        return self._naive_id

    def to_valid_string(self) -> str:
        """
        Returns valid record_id, with special braces if needed
        Checks and adds backticks if id is not in simple form(a..zA..Z0-9), otherwise just returns naive_id
        If id is valid uuid, returns with u prefix like table:u'id'
        """
        is_complicated_format = any(e not in ALPHABET for e in self._id_part.lower())
        if not is_complicated_format:
            return self._naive_id
        if len(self._id_part) == 36 and "-" in self._id_part:
            return self.to_uid_string()
        return self.to_string_with_backticks()

    def to_prefixed_string(self) -> str:
        """
        Return record id with r prefix like r'table:id'
        """
        return f"r'{self._naive_id}'"

    def to_string_with_backticks(self) -> str:
        """
        Return record id with backticks like ""table:`id`"
        """
        return self._string

    def to_uid_string(self) -> str:
        """
        Return record id with u-prefix like table:u'id'
        """
        return self._uid
