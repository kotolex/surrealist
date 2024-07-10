import json
from typing import Optional

from surrealist.ql.statements.statement import FinishedStatement, Statement


# This module contains classes with duplicate code. This is made specifically for IDE hints

class Comment(FinishedStatement):
    """
    Represents a COMMENT clause in DEFINE INDEX statement.
    """
    def __init__(self, statement: Statement, text: str):
        super().__init__(statement)
        self._text = text

    def _clean_str(self):
        text = f" COMMENT {json.dumps(self._text)}" if self._text else ""
        return f"{self._statement._clean_str()}{text}"


class CanUseComment:
    def comment(self, comment: str) -> Comment:
        """
        Adds COMMENT statement to the query
        :param comment: comment text
        :return: self
        """
        return Comment(self, comment)


class Unique(FinishedStatement, CanUseComment):
    """
    Represents UNIQUE statement

    Refer to:
    https://surrealdb.com/docs/surrealdb/surrealql/statements/define/indexes#unique-index
    """

    def _clean_str(self):
        return f"{self._statement._clean_str()} UNIQUE"


class SearchAnalyzer(FinishedStatement, CanUseComment):
    """
    Represents SEARCH ANALYZER statement

    Refer to:
    https://surrealdb.com/docs/surrealdb/surrealql/statements/define/indexes#full-text-search-index
    """

    def __init__(self, statement: Statement, name: str):
        super().__init__(statement)
        self._name = name
        self._bm25 = None
        self._high = False

    def highlights(self) -> "SearchAnalyzer":
        """
        Used for highlights the matching keywords

        Refer to:
        https://surrealdb.com/docs/surrealdb/reference-guide/full-text-search#highlighting

        :return: SearchAnalyzer object
        """
        self._high = True
        return self

    def bm25(self, k1: Optional[float] = None, b: Optional[float] = None) -> "SearchAnalyzer":
        """
        Use ranking algorithm Best Match 25 (Add BM25 clause to a final statement)
        BM25 [(@k1, @b)]

        Note: you should specify both parameters (k1, b) or none of them. If you specify only one of them,
        the other will be equal to 0.0

        :param k1: optional parameter for BM25
        :param b: optional parameter for BM25
        :return: SearchAnalyzer object
        """
        self._bm25 = (k1, b)
        if (k1, b) != (None, None):
            self._bm25 = (k1 or 0.0, b or 0.0)
        return self

    def _clean_str(self):
        hl = "" if not self._high else " HIGHLIGHTS"
        bm25 = ""
        if self._bm25:
            pair = self._bm25
            bm25 = " BM25" if pair == (None, None) else f" BM25 {pair[0]} {pair[1]}"
        return f"{self._statement._clean_str()} SEARCH ANALYZER {self._name}{bm25}{hl}"


class MTree(FinishedStatement, CanUseComment):
    """
    Represents MTREE index.
    There is code duplication in this class. This is made specifically for IDE hints

    Refer to:
    https://surrealdb.com/docs/surrealdb/surrealql/statements/define/indexes#vector-search-indexes
    """

    def __init__(self, statement: Statement, dimension_number: int):
        super().__init__(statement)
        self._num = dimension_number
        self._dist = None
        self._capacity = None
        self._type = None

    def distance_euclidean(self) -> "MTree":
        """
        Make index to use Euclidean distance.
        When no function is specified, SurrealDB chooses Euclidean for the default distance function

        Refer to:
        https://surrealdb.com/docs/surrealdb/surrealql/functions/vector#vectordistanceeuclidean

        :return: MTree object
        """
        self._dist = "EUCLIDEAN"
        return self

    def distance_manhattan(self) -> "MTree":
        """
        Make index to use Manhattan distance
        When no function is specified, SurrealDB chooses Euclidean for the default distance function

        Refer to:
        https://surrealdb.com/docs/surrealdb/surrealql/functions/vector#vectordistancemanhattan

        :return: MTree object
        """
        self._dist = "MANHATTAN"
        return self

    def distance_cosine(self) -> "MTree":
        """
        Make index to use Cosine distance
        When no function is specified, SurrealDB chooses Euclidean for the default distance function

        Refer to:
        https://surrealdb.com/docs/surrealdb/surrealql/functions/vector#vectorsimilaritycosine

        :return: MTree object
        """
        self._dist = "COSINE"
        return self

    def distance_minkowski(self) -> "MTree":
        """
        Make index to use Minkowski distance
        When no function is specified, SurrealDB chooses Euclidean for the default distance function

        Refer to:
        https://surrealdb.com/docs/surrealdb/surrealql/functions/vector#vectordistanceminkowski

        :return: MTree object
        """
        self._dist = "MINKOWSKI"
        return self

    def capacity(self, value: int) -> "MTree":
        """
        The CAPACITY clause is used to specify the maximum number of records that can be stored in the index.
        This is useful when you want to limit the number of records stored in the index to optimize performance
        The default value is 40 (on SurrealDB side)

        :param value: integer value for capacity
        :return: MTree object
        """
        self._capacity = value
        return self

    def type_f64(self) -> "MTree":
        """
        The TYPE clause is optional and can be used to specify the data type of the vector.
        Represents 64-bit floating-point numbers (double precision floating-point numbers).

        Refer to:
        https://surrealdb.com/docs/surrealdb/surrealql/statements/define/indexes#types

        :return: MTree object
        """
        self._type = "F64"
        return self

    def type_f32(self) -> "MTree":
        """
        The TYPE clause is optional and can be used to specify the data type of the vector.
        Represents 32-bit floating-point numbers (single precision floating-point numbers).

        Refer to:
        https://surrealdb.com/docs/surrealdb/surrealql/statements/define/indexes#types

        :return: MTree object
        """
        self._type = "F32"
        return self

    def type_i64(self) -> "MTree":
        """
        The TYPE clause is optional and can be used to specify the data type of the vector.
        Represents 64-bit signed integers.

        Refer to:
        https://surrealdb.com/docs/surrealdb/surrealql/statements/define/indexes#types

        :return: MTree object
        """
        self._type = "I64"
        return self

    def type_i32(self) -> "MTree":
        """
        The TYPE clause is optional and can be used to specify the data type of the vector.
        Represents 32-bit signed integers.

        Refer to:
        https://surrealdb.com/docs/surrealdb/surrealql/statements/define/indexes#types

        :return: MTree object
        """
        self._type = "I32"
        return self

    def type_i16(self) -> "MTree":
        """
        The TYPE clause is optional and can be used to specify the data type of the vector.
        Represents 16-bit signed integers.

        Refer to:
        https://surrealdb.com/docs/surrealdb/surrealql/statements/define/indexes#types

        :return: MTree object
        """
        self._type = "I16"
        return self

    def _clean_str(self):
        type_ = f' TYPE {self._type}' if self._type else ''
        dist = f' DIST {self._dist}' if self._dist else ''
        cap = f' CAPACITY {self._capacity}' if self._capacity else ''
        return f"{self._statement._clean_str()} MTREE DIMENSION {self._num}{type_}{dist}{cap}"


class HNSW(FinishedStatement, CanUseComment):
    """
    Represents HNSW index
    There is code duplication in this class. This is made specifically for IDE hints

    Refer to:
    https://surrealdb.com/docs/surrealdb/surrealql/statements/define/indexes#hnsw-hierarchical-navigable-small-world-since-150
    """

    def __init__(self, statement: Statement, dimension_number: int):
        super().__init__(statement)
        self._num = dimension_number
        self._dist = None
        self._efc = None
        self._type = None
        self._m = None

    def type_f64(self) -> "HNSW":
        """
        The TYPE clause is optional and can be used to specify the data type of the vector.
        Represents 64-bit floating-point numbers (double precision floating-point numbers).

        Refer to:
        https://surrealdb.com/docs/surrealdb/surrealql/statements/define/indexes#types

        :return: HNSW object
        """
        self._type = "F64"
        return self

    def type_f32(self) -> "HNSW":
        """
        The TYPE clause is optional and can be used to specify the data type of the vector.
        Represents 32-bit floating-point numbers (single precision floating-point numbers).

        Refer to:
        https://surrealdb.com/docs/surrealdb/surrealql/statements/define/indexes#types

        :return: HNSW object
        """
        self._type = "F32"
        return self

    def type_i64(self) -> "HNSW":
        """
        The TYPE clause is optional and can be used to specify the data type of the vector.
        Represents 64-bit signed integers.

        Refer to:
        https://surrealdb.com/docs/surrealdb/surrealql/statements/define/indexes#types

        :return: HNSW object
        """
        self._type = "I64"
        return self

    def type_i32(self) -> "HNSW":
        """
        The TYPE clause is optional and can be used to specify the data type of the vector.
        Represents 32-bit signed integers.

        Refer to:
        https://surrealdb.com/docs/surrealdb/surrealql/statements/define/indexes#types

        :return: HNSW object
        """
        self._type = "I32"
        return self

    def type_i16(self) -> "HNSW":
        """
        The TYPE clause is optional and can be used to specify the data type of the vector.
        Represents 16-bit signed integers.

        Refer to:
        https://surrealdb.com/docs/surrealdb/surrealql/statements/define/indexes#types

        :return: HNSW object
        """
        self._type = "I16"
        return self

    def distance_euclidean(self) -> "HNSW":
        """
        Make index to use Euclidean distance
        When no function is specified, SurrealDB chooses Euclidean for the default distance function

        Refer to:
        https://surrealdb.com/docs/surrealdb/surrealql/functions/vector#vectordistanceeuclidean

        :return: HNSW object
        """
        self._dist = "EUCLIDEAN"
        return self

    def distance_manhattan(self) -> "HNSW":
        """
        Make index to use Manhattan distance
        When no function is specified, SurrealDB chooses Euclidean for the default distance function

        Refer to:
        https://surrealdb.com/docs/surrealdb/surrealql/functions/vector#vectordistancemanhattan

        :return: HNSW object
        """
        self._dist = "MANHATTAN"
        return self

    def distance_cosine(self) -> "HNSW":
        """
        Make index to use Cosine distance
        When no function is specified, SurrealDB chooses Euclidean for the default distance function

        Refer to:
        https://surrealdb.com/docs/surrealdb/surrealql/functions/vector#vectorsimilaritycosine

        :return: HNSW object
        """
        self._dist = "COSINE"
        return self

    def distance_minkowski(self) -> "HNSW":
        """
        Make index to use Minkowski distance
        When no function is specified, SurrealDB chooses Euclidean for the default distance function

        Refer to:
        https://surrealdb.com/docs/surrealdb/surrealql/functions/vector#vectordistanceminkowski

        :return: HNSW object
        """
        self._dist = "MINKOWSKI"
        return self

    def efc(self, value: int) -> "HNSW":
        """
        Exploration factor during construction. This parameter determines the size of the dynamic list for the nearest
        neighbor candidates during the graph construction phase. A larger efConstruction value leads to a more thorough
        construction, improving the quality and accuracy of the search but increasing construction time.
        The default value is 150 (on SurrealDB side)

        :param value: integer value for EFC
        :return: HNSW object
        """
        self._efc = value
        return self

    def max_connections(self, value: int) -> "HNSW":
        """
        Max Connections per Element. Defines the maximum number of bidirectional links (neighbors) per node in each
        layer of the graph, except for the lowest layer. This parameter controls the connectivity and overall structure
        of the network. Higher values of MM generally improve search accuracy but increase memory usage and
        construction time.
        The default value is 12 (on the SurrealDB side)

        :param value: integer value for MM
        :return: HNSW object
        """
        self._m = value
        return self

    def _clean_str(self):
        type_ = f' TYPE {self._type}' if self._type else ''
        dist = f' DIST {self._dist}' if self._dist else ''
        efc = f' EFC {self._efc}' if self._efc else ''
        m = f' M {self._m}' if self._m else ''
        return f"{self._statement._clean_str()} HNSW DIMENSION {self._num}{type_}{dist}{efc}{m}"


class CanUseIndexTypes:
    """
    Represents interface for index types
    """

    def unique(self) -> Unique:
        """
        Creates unique index

        Refer to:
        https://surrealdb.com/docs/surrealdb/surrealql/statements/define/indexes#unique-index

        :return: Unique object
        """
        return Unique(self)

    def search_analyzer(self, name: str) -> SearchAnalyzer:
        """
        Creates full-text search index

        Refer to:
        https://surrealdb.com/docs/surrealdb/surrealql/statements/define/indexes#full-text-search-index

        :param name: analyzer name
        :return: SearchAnalyzer object
        """
        return SearchAnalyzer(self, name)

    def mtree(self, num_dimension: int) -> MTree:
        """
        Creates vector-search index MTREE

        Refer to:
        https://surrealdb.com/docs/surrealdb/surrealql/statements/define/indexes#vector-search-indexes

        :param num_dimension: number of dimensions
        :return: MTree object
        """
        return MTree(self, num_dimension)

    def hnsw(self, num_dimension: int) -> HNSW:
        """
        Creates vector-search index HNSW

        Refer to:
        https://surrealdb.com/docs/surrealdb/surrealql/statements/define/indexes#hnsw-hierarchical-navigable-small-world-since-150

        :param num_dimension: number of dimensions
        :return: HNSW object
        """
        return HNSW(self, num_dimension)
