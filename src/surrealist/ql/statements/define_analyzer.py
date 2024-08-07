from typing import List

from .define import Define, Connection
from ...utils import OK

ALLOWED_LANGUAGES = ('Arabic', 'Danish', 'Dutch', 'English', 'French', 'German', 'Greek', 'Hungarian',
                     'Italian', 'Norwegian', 'Portuguese', 'Romanian', 'Russian', 'Spanish', 'Swedish',
                     'Tamil', 'Turkish')


class DefineAnalyzer(Define):
    """
    Represents DEFINE ANALYZER statement

    Refer to: https://docs.surrealdb.com/docs/surrealql/statements/define/analyzer

    Example: https://github.com/kotolex/surrealist/blob/master/examples/surreal_ql/database.py

    DEFINE ANALYZER [ OVERWRITE | IF NOT EXISTS ] @name [ TOKENIZERS @tokenizers ] [ FILTERS @filters ]
    [ COMMENT @string ]

    """

    def __init__(self, connection: Connection, name: str):
        super().__init__(connection)
        self._name = name
        self._tokenizers = set()
        self._filters = set()
        self._error = None

    def if_not_exists(self) -> "DefineAnalyzer":
        self._if_not_exists = True
        return self

    def overwrite(self) -> "DefineAnalyzer":
        """
        Adds OVERWRITE statement to the query
        :return: self
        """
        self._if_not_exists = False
        return self

    def tokenizer_blank(self) -> "DefineAnalyzer":
        """
        Represents TOKENIZERS blank statement
        The blank tokenizer breaks down a text into tokens by creating a new token each time it encounters a space,
        tab, or newline character. It's a straightforward way to split text into words or chunks based on whitespace.
        For example, if you had the text "hello world", the blank tokenizer would create two tokens,
        ["hello" and "world"].

        Refer to: https://surrealdb.com/docs/surrealdb/2.x/surrealql/statements/define/analyzer#blank
        """
        self._tokenizers.add("blank")
        return self

    def tokenizer_camel(self) -> "DefineAnalyzer":
        """
        Represents TOKENIZERS camel statement
        The camel tokenizer is used for identifying and creating tokens when the next character in the text is
        uppercase. This is particularly useful for processing camelCase or PascalCase text, common in programming,
        to split them into meaningful words.
        For example, if you had the text "helloWorld", the camel tokenizer would create two tokens, ["hello", "World"].

        Refer to: https://surrealdb.com/docs/surrealdb/2.x/surrealql/statements/define/analyzer#camel
        """
        self._tokenizers.add("camel")
        return self

    def tokenizer_class(self) -> "DefineAnalyzer":
        """
        Represents TOKENIZERS class statement
        The class tokenizer segments text into tokens by detecting changes (digit, letter, punctuation, blank) in the
        Unicode class of characters. It creates a new token when the character class changes, distinguishing between
        digits, letters, punctuation, and blanks. This allows for flexible tokenization based on character types.
        For example, if you had the text "123abc!XYZ", the class tokenizer would create four tokens,
        ["123", "abc", "!", "XYZ"].

        Refer to: https://surrealdb.com/docs/surrealdb/2.x/surrealql/statements/define/analyzer#class
        """
        self._tokenizers.add("class")
        return self

    def tokenizer_punct(self) -> "DefineAnalyzer":
        """
        Represents TOKENIZERS punct statement
        The punct tokenizer generates tokens by breaking the text whenever a punctuation character is encountered.
        It's suitable for tokenizing sentences or breaking text into smaller units based on punctuation marks.
        For example, if you had the text "Hello, World!", the punct tokenizer would create four tokens,
        ["Hello", ",", "World", "!"]

        Refer to: https://surrealdb.com/docs/surrealdb/2.x/surrealql/statements/define/analyzer#punct
        """
        self._tokenizers.add("punct")
        return self

    def filter_ascii(self) -> "DefineAnalyzer":
        """
        Represents FILTERS ascii statement
        The ascii filter is responsible for processing tokens by replacing or removing diacritical marks (accents and
        special characters) from the text. It helps standardize text by converting accented characters to their basic
        ASCII equivalents, making it more suitable for various text analysis tasks.
        For example, if you had the text "résumé café", the ascii filter would create two tokens, ["resume", "cafe"]

        Refer to: https://surrealdb.com/docs/surrealdb/2.x/surrealql/statements/define/analyzer#ascii
        """
        self._filters.add("ascii")
        return self

    def filter_lowercase(self) -> "DefineAnalyzer":
        """
        Represents FILTERS lowercase statement
        The lowercase filter converts tokens to lowercase, ensuring that text is consistently in lowercase format.
        This is often used to make text case-insensitive for search and analysis purposes.
        For example, if you had the text "Hello World", the lowercase filter would create two tokens, ["hello", "world"]

        Refer to: https://surrealdb.com/docs/surrealdb/2.x/surrealql/statements/define/analyzer#lowercase
        """
        self._filters.add("lowercase")
        return self

    def filter_uppercase(self) -> "DefineAnalyzer":
        """
        Represents FILTERS uppercase statement
        The uppercase filter converts tokens to uppercase, ensuring text consistency in uppercase format. It can be
        useful when case-insensitivity is required for specific analysis or search operations.
        For example, if you had the text "Hello World", the uppercase filter would create two tokens, ["HELLO", "WORLD"]

        Refer to: https://surrealdb.com/docs/surrealdb/2.x/surrealql/statements/define/analyzer#uppercase
        """
        self._filters.add("uppercase")
        return self

    def filter_ngram(self, minimum: int, maximum: int) -> "DefineAnalyzer":
        """
        Represents FILTERS ngram(min,max) statement
        The ngram filter is used to create a sequence of 'n' tokens from a given sample of text or speech. These items
        can be syllables, letters, words or base pairs according to the application. It accepts two parameters min and
        max which indicates that you want to create n-grams starting from min to size of max.
        For example, if you had the text "apple banana", the ngram filter would create these tokens:
        ["a", "p", "p", "l", "e", "ap", "pp", "pl", "le", "app", "ppl", "ple", "b", "a", "n", "a", "n", "a", "ba", "an",
         "na", "an", "na", "ban", "ana", "nan", "ana"]

        Refer to: https://surrealdb.com/docs/surrealdb/2.x/surrealql/statements/define/analyzer#ngramminmax
        """
        self._filters.add(f"ngram({minimum},{maximum})")
        return self

    def filter_edgengram(self, minimum: int, maximum: int) -> "DefineAnalyzer":
        """
        Represents FILTERS edgengram(min,max) statement
        The edgengram filter is used to create tokens that represent prefixes of terms. It generates a sequence of
        tokens that gradually build up a term, which can be useful for autocomplete or searching based on partial words.
        It accepts two parameters min and max which define the minimum and maximum amount of characters in the prefix.
        For example, if you had the text "apple banana", the edgengram filter would create six tokens,
        ["a", "ap", "app", "b", "ba", "ban"]

        Refer to: https://surrealdb.com/docs/surrealdb/2.x/surrealql/statements/define/analyzer#edgengramminmax
        """
        self._filters.add(f"edgengram({minimum},{maximum})")
        return self

    def filter_snowball(self, language: str) -> "DefineAnalyzer":
        """
        Represents FILTERS snowball(language) statement
        The snowball filter applies Snowball stemming to tokens, reducing them to their root form and converts the case
        to lowercase. The following supported languages can be passed as a parameter in snowball: Arabic, Danish, Dutch,
        English, French, German, Greek, Hungarian, Italian, Norwegian, Portuguese, Romanian, Russian, Spanish, Swedish,
        Tamil, Turkish.
        For example, if you had the text "running cats", the snowball filter would create two tokens, ["run", "cat"]

        Refer to: https://surrealdb.com/docs/surrealdb/2.x/surrealql/statements/define/analyzer#snowballlanguage
        """
        if not any(language.lower() == lang.lower() for lang in ALLOWED_LANGUAGES):
            self._error = language
        self._filters.add(f"snowball({language})")
        return self

    def validate(self) -> List[str]:
        if self._error:
            return [f"{self._error} is not in {ALLOWED_LANGUAGES}"]
        return [OK]

    def _clean_str(self):
        tok = "" if not self._tokenizers else f" TOKENIZERS {', '.join(sorted(self._tokenizers))}"
        filters = "" if not self._filters else f" FILTERS {', '.join(sorted(self._filters))}"
        return f"DEFINE ANALYZER{self._exists()} {self._name}{tok}{filters}{self._comment()}"
