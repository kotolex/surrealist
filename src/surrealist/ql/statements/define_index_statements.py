from surrealist.ql.statements.statement import FinishedStatement, Statement


class Unique(FinishedStatement):
    """
    Represents UNIQUE statement

    Refer to:
    https://surrealdb.com/docs/surrealdb/surrealql/statements/define/indexes#unique-index
    """

    def _clean_str(self):
        return f"{self._statement._clean_str()} UNIQUE"


class SearchAnalyzer(FinishedStatement):
    """
    Represents SEARCH ANALYZER statement

    Refer to:
    https://surrealdb.com/docs/surrealdb/surrealql/statements/define/indexes#full-text-search-index
    """

    def __init__(self, statement: Statement, name: str, use: str = "BM25", highlights: bool = True):
        super().__init__(statement)
        self._name = name
        self._use = use
        self._high = highlights

    def _clean_str(self):
        hl = "" if not self._high else " HIGHLIGHTS"
        return f"{self._statement._clean_str()} SEARCH ANALYZER {self._name} {self._use}{hl}"


class MTree(FinishedStatement):
    """
    Represents MTREE index

    Refer to:
    https://surrealdb.com/docs/surrealdb/surrealql/statements/define/indexes#vector-search-indexes
    """

    def __init__(self, statement: Statement, dimension_number: int):
        super().__init__(statement)
        self._num = dimension_number
        self._dist = None
        self._capacity = None
        self._type = None

    def type(self, mtree_type: str) -> "MTree":
        self._type = mtree_type
        return self

    def distance_euclidean(self) -> "MTree":
        self._dist = "EUCLIDEAN"
        return self

    def distance_manhattan(self) -> "MTree":
        self._dist = "MANHATTAN"
        return self

    def distance_cosine(self) -> "MTree":
        self._dist = "COSINE"
        return self

    def distance_minkowski(self) -> "MTree":
        self._dist = "MINKOWSKI"
        return self

    def capacity(self, capacity_value: str) -> "MTree":
        self._capacity = capacity_value
        return self

    def _clean_str(self):
        type_ = f' TYPE {self._type}' if self._type else ''
        dist = f' DIST {self._dist}' if self._dist else ''
        cap = f' CAPACITY {self._capacity}' if self._capacity else ''
        return f"{self._statement._clean_str()} MTREE DIMENSION {self._num}{type_}{dist}{cap}"


class HNSW(FinishedStatement):
    """
    Represents HNSW index

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

    def type(self, mtree_type: str) -> "HNSW":
        self._type = mtree_type
        return self

    def distance_euclidean(self) -> "HNSW":
        self._dist = "EUCLIDEAN"
        return self

    def distance_manhattan(self) -> "HNSW":
        self._dist = "MANHATTAN"
        return self

    def distance_cosine(self) -> "HNSW":
        self._dist = "COSINE"
        return self

    def distance_minkowski(self) -> "HNSW":
        self._dist = "MINKOWSKI"
        return self

    def efc(self, value: str) -> "HNSW":
        self._efc = value
        return self

    def m(self, value: str) -> "HNSW":
        self._m = value
        return self

    def _clean_str(self):
        type_ = f' TYPE {self._type}' if self._type else ''
        dist = f' DIST {self._dist}' if self._dist else ''
        efc = f' EFC {self._efc}' if self._efc else ''
        m = f' M {self._m}' if self._m else ''
        return f"{self._statement._clean_str()} HNSW DIMENSION {self._num}{type_}{dist}{efc}{m}"


class CanUseIndexTypes:
    def unique(self) -> Unique:
        """
        Creates unique index

        Refer to:
        https://surrealdb.com/docs/surrealdb/surrealql/statements/define/indexes#unique-index

        :return: Unique object
        """
        return Unique(self)

    def search_analyzer(self, name: str, use: str = "BM25", highlights: bool = True) -> SearchAnalyzer:
        """
        Creates full-text search index

        Refer to:
        https://surrealdb.com/docs/surrealdb/surrealql/statements/define/indexes#full-text-search-index

        :param name: analyzer name
        :param use: model to use
        :param highlights: use HIGHLIGHT if True
        :return: SearchAnalyzer object
        """
        return SearchAnalyzer(self, name, use, highlights)

    def mtree(self, num_dimension: int) -> MTree:
        """
        Creates vector-search index

        Refer to:
        https://surrealdb.com/docs/surrealdb/surrealql/statements/define/indexes#vector-search-indexes

        :param num_dimension: number of dimensions
        :return: MTree object
        """
        return MTree(self, num_dimension)

    def hnsw(self, num_dimension: int) -> HNSW:
        """
        Creates vector-search index

        Refer to:
        https://surrealdb.com/docs/surrealdb/surrealql/statements/define/indexes#hnsw-hierarchical-navigable-small-world-since-150

        :param num_dimension: number of dimensions
        :return: MTree object
        """
        return HNSW(self, num_dimension)
