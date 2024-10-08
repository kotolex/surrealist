from unittest import TestCase, main

from tests.integration_tests.utils import URL
from surrealist import Surreal


names = [
    ("array::add", '["one", "two"], "three"', ['one', 'two', 'three']),
    ("array::all", "[ 1, 2, 3, NONE, 'SurrealDB', 5 ]", False),
    ("array::any", "[ 1, 2, 3, NONE, 'SurrealDB', 5 ]", True),
    ("array::at", "['s', 'u', 'r', 'r', 'e', 'a', 'l'], 2", "r"),
    ("array::boolean_and", '["true", "false", 1, 1], ["true", "true", 0, "true"]', [True, True, False, True]),
    ("array::boolean_and", '[true, true], [false]', [False, False]),
    ("array::boolean_or", '[false, true, false, true], [false, false, true, true]', [False, True, True, True]),
    ("array::boolean_xor", '[false, true, false, true], [false, false, true, true]', [False, True, True, False]),
    ("array::boolean_not", '[ false, true, 0, 1 ]', [True, False, True, False]),
    ("array::combine", '[1,2], [2,3]', [[1, 2], [1, 3], [2, 2], [2, 3]]),
    ("array::complement", '[1,2,3,4], [3,4,5,6]', [1, 2]),
    ("array::concat", '[1,2,3,4], [3,4,5,6]', [1, 2, 3, 4, 3, 4, 5, 6]),
    ("array::clump", '[0,1,2,3], 2', [[0, 1], [2, 3]]),
    ("array::difference", '[1,2,3,4], [3,4,5,6]', [1, 2, 5, 6]),
    ("array::distinct", '[ 1, 2, 1, 3, 3, 4 ]', [1, 2, 3, 4]),
    ("array::flatten", "[ [1,2], [3, 4], 'SurrealDB', [5, 6, [7, 8]] ]", [1, 2, 3, 4, 'SurrealDB', 5, 6, [7, 8]]),
    ("array::find_index", "['a', 'b', 'c', 'b', 'a'], 'b'", 1),
    ("array::filter", "[ 1, 2, 1, 3, 3, 4 ], 1", [1, 1]),
    ("array::filter_index", "['a', 'b', 'c', 'b', 'a'], 'b'", [1, 3]),
    ("array::find", "['a', 'b', 'c', 'b', 'a'], 'b'", 'b'),
    ("array::first", "[ 's', 'u', 'r', 'r', 'e', 'a', 'l' ]", "s"),
    ("array::group", "[1, 2, 3, 4, [3,5,6], [2,4,5,6], 7, 8, 8, 9]", [1, 2, 3, 4, 5, 6, 7, 8, 9]),
    ("array::insert", "[1,2,3,4], 5, 2", [1, 2, 5, 3, 4]),
    ("array::intersect", "[1,2,3,4], [3,4,5,6]", [3, 4]),
    ("array::join", '["again", "again", "again"], " and "', "again and again and again"),
    ("array::last", "[ 's', 'u', 'r', 'r', 'e', 'a', 'l' ]", "l"),
    ("array::len", '[ 1, 2, 1, null, "something", 3, 3, 4, 0 ]', 9),
    ("array::logical_and", '[true, false, true, false], [true, true, false, false]', [True, False, False, False]),
    ("array::logical_or", '[true, false, true, false], [true, true, false, false]', [True, True, True, False]),
    ("array::logical_xor", '[true, false, true, false], [true, true, false, false]', [False, True, True, False]),
    ("array::logical_and", '[0, 1], []', [0, None]),
    ("array::max", '[0, 1, 2]', 2),
    ("array::min", '[0, 1, 2]', 0),
    ("array::matches", '[0, 1, 2], 1', [False, True, False]),
    ("array::matches", '[{id: "ohno:0"}, {id: "ohno:1"}], {id: "ohno:1"}', [False, True]),
    ("array::pop", '[ 1, 2, 3, 4 ]', 4),
    ("array::prepend", '[1,2,3,4], 5', [5, 1, 2, 3, 4]),
    ("array::push", '[1,2,3,4], 5', [1, 2, 3, 4, 5]),
    ("array::remove", '[1,2,3,4,5], 2', [1, 2, 4, 5]),
    ("array::remove", '[1,2,3,4,5], -2', [1, 2, 3, 5]),
    ("array::reverse", '[ 1, 2, 3, 4, 5 ]', [5, 4, 3, 2, 1]),
    ("array::sort", '[ 1, 2, 1, null, "something", 3, 3, 4, 0 ]', [None, 0, 1, 1, 2, 3, 3, 4, "something"]),
    ("array::sort::asc", '[ 1, 2, 1, null, "something", 3, 3, 4, 0 ]', [None, 0, 1, 1, 2, 3, 3, 4, "something"]),
    ("array::sort::desc", '[ 1, 2, 1, null, "something", 3, 3, 4, 0 ]', ["something", 4, 3, 3, 2, 1, 1, 0, None]),
    ("array::slice", '[ 1, 2, 3, 4, 5 ], 1, 2', [2, 3]),
    ("array::transpose", '[[0, 1], [2, 3]]', [[0, 2], [1, 3]]),
    ("array::union", '[1,2,1,6], [1,3,4,5,6]', [1, 2, 6, 3, 4, 5]),
    ("array::windows", '[1,2,3,4], 2', [[1, 2], [2, 3], [3, 4]]),
    ("array::windows", '[1,2,3,4], 5', []),
    ("count", '', 1),
    ("count", '"true"', 1),
    ("count", '10 > 15', 0),
    ("crypto::md5", '"tobie"', "4768b3fc7ac751e03a614e2349abf3bf"),
    ("crypto::sha1", '"tobie"', "c6be709a1b6429472e0c5745b411f1693c4717be"),
    ("crypto::sha256", '"tobie"', "33fe1859daba927ea5674813adc1cf34b9e2795f2b7e91602fae19c0d0c493af"),
    ("crypto::sha512", '"tobie"',
     "39f0160c946c4c53702112d6ef3eea7957ea8e1c78787a482a89f8b0a8860a20ecd543432e4a187d9fdcd1c415cf61008e51a7e8bf2f22ac77e458789c9cdccc"),
    ("crypto::argon2::compare",
     '"$argon2id$v=19$m=4096,t=3,p=1$pbZ6yJ2rPJKk4pyEMVwslQ$jHzpsiB+3S/H+kwFXEcr10vmOiDkBkydVCSMfRxV7CA", "this is a strong password"',
     True),
    ("crypto::bcrypt::compare",
     '"$2b$12$OD7hrr1Hycyk8NUwOekYY.cogCICpUnwNvDZ9NiC1qCPHzpVAQ9BO", "this is a strong password"', True),
    ("duration::days", '3w', 21),
    ("duration::hours", '3w', 504),
    ("duration::micros", '3w', 1814400000000),
    ("duration::millis", '3w', 1814400000),
    ("duration::secs", '3w', 1814400),
    ("duration::weeks", '3w', 3),
    ("duration::years", '300w', 5),
    ("duration::from::hours", '3', "3h"),
    ("duration::from::micros", '3', "3µs"),
    ("duration::from::millis", '3', "3ms"),
    ("duration::from::mins", '3', "3m"),
    ("duration::from::nanos", '3', "3ns"),
    ("duration::from::secs", '3', "3s"),
    ("duration::from::weeks", '3', "3w"),
    ("math::abs", '-13.746189', 13.746189),
    ("math::acos", '0.5', 1.0471975511965979),
    ("math::acot", '1', 0.7853981633974483),
    ("math::asin", '0.5', 0.5235987755982989),
    ("math::atan", '1', 0.7853981633974483),
    ("math::ln", '10', 2.302585092994046),
    ("math::clamp", '1, 5, 10', 5),
    ("math::log", '100, 10', 2.0),
    ("math::log10", '1000', 3.0),
    ("math::log2", '8', 3.0),
    ("math::lerp", '0, 10, 0.5', 5.0),
    ("math::lerpangle", '0, 180, 0.5', 90.0),
    ("math::cos", '1', 0.5403023058681398),
    ("math::cot", '1', 0.6420926159343306),
    ("math::deg2rad", '180', 3.141592653589793),
    ("math::rad2deg", '3.141592653589793', 180.0),
    ("math::sign", '-42', -1),
    ("math::sin", '1', 0.8414709848078965),
    ("math::tan", '1', 1.5574077246549023),
    ("math::bottom", '[1, 2, 3], 2', [2, 1]),
    ("math::ceil", '13.146572', 14.0),
    ("math::fixed", '13.146572, 2', 13.15),
    ("math::floor", '13.746189', 13.0),
    ("math::interquartile", '[ 1, 40, 60, 10, 2, 901 ]', 51.0),
    ("math::max", '[ 26.164, 13.746189, 23, 16.4, 41.42 ]', 41.42),
    ("math::max", '[ 26.164, 13.746189, 23, 16.4, 41.42 ]', 41.42),
    ("math::min", '[ 26.164, 13.746189, 23, 16.4, 41.42 ]', 13.746189),
    ("math::median", '[ 26.164, 13.746189, 23, 16.4, 41.42 ]', 23.0),
    ("math::midhinge", '[ 1, 40, 60, 10, 2, 901 ]', 29.5),
    ("math::mode", '[ 1, 40, 60, 10, 2, 901 ]', 901),
    ("math::nearestrank", '[1, 40, 60, 10, 2, 901], 50', 40),
    ("math::percentile", '[1, 40, 60, 10, 2, 901], 50', 25.0),
    ("math::product", '[ 26.164, 13.746189, 23, 16.4, 41.42 ]', 5619119.004884841),
    ("math::round", '13.53124', 14.0),
    ("math::spread", '[ 1, 40, 60, 10, 2, 901 ]', 900),
    ("math::sqrt", '16', 4.0),
    ("math::stddev", '[ 1, 40, 60, 10, 2, 901 ]', 359.37167389765153),
    ("math::sum", '[ 26.164, 13.746189, 23, 16.4, 41.42 ]', 120.73018900000001),
    ("math::top", '[1, 40, 60, 10, 2, 901], 3', [40, 901, 60]),
    ("math::variance", '[ 1, 40, 60, 10, 2, 901 ]', 129148.0),
    ("math::trimean", '[ 1, 40, 60, 10, 2, 901 ]', 27.25),
    ("record::id", 'person:tobie', "tobie"),
    ("record::tb", 'person:tobie', "person"),
    ("record::exists", 'person:tobie', False),
    ("parse::email::host", '"info@surrealdb.com"', "surrealdb.com"),
    ("parse::email::user", '"info@surrealdb.com"', "info"),
    ("parse::url::domain", '"https://surrealdb.com:80/features?some=option#fragment"', "surrealdb.com"),
    ("parse::url::host", '"https://surrealdb.com:80/features?some=option#fragment"', "surrealdb.com"),
    ("parse::url::fragment", '"https://surrealdb.com:80/features?some=option#fragment"', "fragment"),
    ("parse::url::path", '"https://surrealdb.com:80/features?some=option#fragment"', "/features"),
    ("parse::url::port", '"https://surrealdb.com:80/features?some=option#fragment"', "80"),
    ("parse::url::query", '"https://surrealdb.com:80/features?some=option#fragment"', "some=option"),
    ("string::concat", "'this', ' ', 'is', ' ', 'a', ' ', 'test'", "this is a test"),
    ("string::contains", "'abcdefg', 'cde'", True),
    ("string::ends_with", "'some test', 'test'", True),
    ("string::join", "', ', 'a', 'list', 'of', 'items'", "a, list, of, items"),
    ("string::len", "'this is a test'", 14),
    ("string::lowercase", "'THIS IS A TEST'", "this is a test"),
    ("string::repeat", "'test', 3", "testtesttest"),
    ("string::replace", "'this is a test', 'a test', 'awesome'", "this is awesome"),
    ("string::reverse", "'this is a test'", "tset a si siht"),
    ("string::slice", "'this is a test', 10, 4", "test"),
    ("string::slug", "'SurrealDB has launched #database #awesome'", "surrealdb-has-launched-database-awesome"),
    ("string::split", "'this, is, a, list', ', '", ["this", "is", "a", "list"]),
    ("string::starts_with", "'some test', 'some'", True),
    ("string::trim", "'    this is a test    '", "this is a test"),
    ("string::uppercase", "'this is a test'", "THIS IS A TEST"),
    ("string::words", "'this is a test'", ["this", "is", "a", "test"]),
    ("string::is::alphanum", "'ABC123'", True),
    ("string::is::alpha", "'ABCDEF'", True),
    ("string::is::ascii", "'ABC123'", True),
    ("string::is::datetime", '"2015-09-05 23:56:04", "%Y-%m-%d %H:%M:%S"', True),
    ("string::is::domain", "'surrealdb.com'", True),
    ("string::is::email", "'info@surrealdb.com'", True),
    ("string::is::hexadecimal", "'ff009e'", True),
    ("string::is::ip", "'192.168.1.1'", True),
    ("string::is::ipv4", "'192.168.1.1'", True),
    ("string::is::ipv6", "'192.168.1.1'", False),
    ("string::is::latitude", "'-0.118092'", True),
    ("string::is::longitude", "'51.509865'", True),
    ("string::is::numeric", "'1484091748'", True),
    ("string::is::record", "'person:test'", True),
    ("string::is::record", "'person:test', 'person'", True),
    ("string::is::record", "'person:test', 'other'", False),
    ("string::is::record", "'not a record'", False),
    ("string::is::semver", '"1.0.0"', True),
    ("string::is::url", '"https://surrealdb.com"', True),
    ("string::is::uuid", '"018a6680-bef9-701b-9025-e1754f296a0f"', True),
    ("string::html::encode", '"<h1>Safe Title</h1>"', '&lt;h1&gt;Safe&#32;Title&lt;&#47;h1&gt;'),
    ("string::html::sanitize", '"<h1>Safe Title</h1>"', '<h1>Safe Title</h1>'),
    ("string::semver::compare", '"1.0.0", "1.3.5"', -1),
    ("string::semver::major", '"3.2.6"', 3),
    ("string::semver::minor", '"3.2.6"', 2),
    ("string::semver::patch", '"3.2.6"', 6),
    ("string::semver::inc::major", '"1.2.3"', "2.0.0"),
    ("string::semver::inc::minor", '"1.2.3"', "1.3.0"),
    ("string::semver::inc::patch", '"1.2.3"', "1.2.4"),
    ("string::semver::set::major", '"1.2.3", 9', "9.2.3"),
    ("string::semver::set::minor", '"1.2.3", 9', "1.9.3"),
    ("string::semver::set::patch", '"1.2.3", 9', "1.2.9"),
    ("time::day", 'd"2021-11-01T08:30:17+00:00"', 1),
    ("time::is::leap_year", 'd"1988-11-01T08:30:17+00:00"', True),
    ("time::floor", 'd"2021-11-01T08:30:17+00:00", 1w', "2021-10-28T00:00:00Z"),
    ("time::format", 'd"2021-11-01T08:30:17+00:00", "%Y-%m-%d"', "2021-11-01"),
    ("time::group", 'd"2021-11-01T08:30:17+00:00", "year"', "2021-01-01T00:00:00Z"),
    ("time::hour", 'd"2021-11-01T08:30:17+00:00"', 8),
    ("time::max", '[ d"1987-06-22T08:30:45Z", d"1988-06-22T08:30:45Z" ]', '1988-06-22T08:30:45Z'),
    ("time::min", '[ d"1987-06-22T08:30:45Z", d"1988-06-22T08:30:45Z" ]', "1987-06-22T08:30:45Z"),
    ("time::minute", 'd"2021-11-01T08:30:17+00:00"', 30),
    ("time::month", 'd"2021-11-01T08:30:17+00:00"', 11),
    ("time::nano", 'd"2021-11-01T08:30:17+00:00"', 1635755417000000000),
    ("time::second", 'd"2021-11-01T08:30:17+00:00"', 17),
    ("time::round", 'd"2021-11-01T08:30:17+00:00", 1w', "2021-11-04T00:00:00Z"),
    ("time::unix", 'd"2021-11-01T08:30:17+00:00"', 1635755417),
    ("time::wday", 'd"2021-11-01T08:30:17+00:00"', 1),
    ("time::week", 'd"2021-11-01T08:30:17+00:00"', 44),
    ("time::yday", 'd"2021-11-01T08:30:17+00:00"', 305),
    ("time::year", 'd"2021-11-01T08:30:17+00:00"', 2021),
    ("time::from::micros", '1000000', "1970-01-01T00:00:01Z"),
    ("time::from::millis", '1000', "1970-01-01T00:00:01Z"),
    ("time::from::secs", '1000', "1970-01-01T00:16:40Z"),
    ("time::from::unix", '1000', "1970-01-01T00:16:40Z"),
    ("type::bool", 'true', True),
    ("type::datetime", '"2022-04-27T18:12:27+00:00"', "2022-04-27T18:12:27Z"),
    ("type::decimal", '"12345"', 12345),
    ("type::duration", '"4h"', '4h'),
    ("type::duration", '"4h"', '4h'),
    ("type::float", '"12345"', 12345.0),
    ("type::int", '"12345"', 12345),
    ("type::number", '"12345"', 12345),
    ("type::point", '[ 51.509865, -0.118092 ]', {'type': 'Point', 'coordinates': [51.509865, -0.118092]}),
    ("type::string", '12345', "12345"),
    ("type::table", '"ert"', "ert"),
    ("type::thing", '"one","two"', 'one:two'),
    # https://github.com/surrealdb/surrealdb/issues/4639
    # ("type::range", '"product_price","10", "100", { begin: "excluded", end: "included" }', "{'tb': 'product_price', 'beg': {'Excluded': {'String': '10'}}, 'end': {'Included': {'String': '100'}}}"),
    ("type::is::array", "[ 'a', 'b', 'c' ]", True),
    ("type::is::array", "12345", False),
    ("type::is::bool", "true", True),
    ("type::is::bytes", '"I am not bytes"', False),
    ("type::is::collection", '"I am not bytes"', False),
    ("type::is::line", '"I am not bytes"', False),
    ("type::is::multiline", '"I am not bytes"', False),
    ("type::is::multipoint", '"I am not bytes"', False),
    ("type::is::multipolygon", '"I am not bytes"', False),
    ("type::is::polygon", '"I am not bytes"', False),
    ("type::is::none", 'NONE', True),
    ("type::is::null", 'NULL', True),
    ("type::is::datetime", 'time::now()', True),
    ("type::is::decimal", '<decimal> 13.5719384719', True),
    ("type::is::duration", '"1970-01-01T00:00:00"', False),
    ("type::is::float", '<float> 41.5', True),
    ("type::is::int", '41', True),
    ("type::is::geometry", '(-0.118092, 51.509865)', True),
    ("type::is::point", '(-0.118092, 51.509865)', True),
    ("type::is::number", '123', True),
    ("type::is::object", "{ 'hello': 'world' }", True),
    ("type::is::object", "{ hello: 'world' }", True),
    ("type::is::record", "user:tobie", True),
    ("type::is::record", "user:tobie, 'test'", False),
    ("type::is::record", "user:tobie, 'user'", True),
    ("type::is::string", "'user'", True),
    ("type::is::uuid", 'u"018a6680-bef9-701b-9025-e1754f296a0f"', True),
    ("value::diff", '"tobie", "tobias"', [{'op': 'change', 'path': '/', 'value': '@@ -1,5 +1,6 @@\n tobi\n-e\n+as\n'}]),
    ("vector::add", '[1, 2, 3], [1, 2, 3]', [2, 4, 6]),
    ("vector::angle", '[5, 10, 15], [10, 5, 20]', 0.36774908225917935),
    ("vector::cross", '[1, 2, 3], [4, 5, 6]', [-3, 6, -3]),
    ("vector::divide", '[10, -20, 30, 0], [0, -1, 2, -3]', [None, 20, 15, 0]),
    ("vector::dot", '[1, 2, 3], [1, 2, 3]', 14),
    ("vector::magnitude", '[ 1, 2, 3, 3, 3, 4, 5 ]', 8.54400374531753),
    ("vector::multiply", '[1, 2, 3], [1, 2, 3]', [1, 4, 9]),
    ("vector::normalize", '[ 4, 3 ]', [0.8, 0.6]),
    ("vector::project", '[1, 2, 3], [4, 5, 6]', [1.6623376623376624, 2.077922077922078, 2.4935064935064934]),
    ("vector::subtract", '[4, 5, 6], [3, 2, 1]', [1, 3, 5]),
    ("vector::distance::chebyshev", '[2, 4, 5, 3, 8, 2], [3, 1, 5, -3, 7, 2]', 6.0),
    ("vector::distance::euclidean", '[10, 50, 200], [400, 100, 20]', 432.43496620879307),
    ("vector::distance::hamming", '[1, 2, 2], [1, 2, 3]', 1),
    ("vector::distance::manhattan", '[10, 20, 15, 10, 5], [12, 24, 18, 8, 7]', 13),
    ("vector::distance::minkowski", '[10, 20, 15, 10, 5], [12, 24, 18, 8, 7], 3', 4.862944131094279),
    ("vector::similarity::cosine", '[10, 50, 200], [400, 100, 20]', 0.15258215962441316),
    ("vector::similarity::jaccard", '[0,1,2,5,6], [0,2,3,4,5,7,9]', 0.3333333333333333),
    ("vector::similarity::pearson", '[1,2,3], [1,5,7]', 0.9819805060619659),
]

functions = [
    ("rand", ''),
    ("rand::bool", ''),
    ("rand::enum", "'one', 'two', 3, 4.15385, 'five', true"),
    ("rand::float", ""),
    ("rand::float", "10, 15"),
    ("rand::guid", ""),
    ("rand::guid", "10"),
    ("rand::int", ""),
    ("rand::int", "10, 15"),
    ("rand::string", "10"),
    ("rand::string", ""),
    ("rand::string", "10,15"),
    ("rand::time", ""),
    ("rand::time", "10,15"),
    ("rand::uuid", ""),
    ("rand::uuid::v4", ""),
    ("rand::uuid::v7", ""),
    ("rand::ulid", ""),
    ("session::db", ""),
    ("session::id", ""),
    ("session::ip", ""),
    ("session::ns", ""),
    ("session::origin", ""),
    ("session::ns", ""),
    ("sleep", "100ms"),
    ("time::now", ""),
    ("time::timezone", ""),

]

variables = [
    "$auth",
    "$token",
    "$Scope",
    "$session",
    "$before",
    "$after",
    "$value",
    "$input",
    "$this",
    "$parent",
    "$event",
]

constants = [
    ("MATH::E", 2.718281828459045),
    ("MATH::FRAC_1_PI", 0.3183098861837907),
    ("MATH::FRAC_1_SQRT_2", 0.7071067811865476),
    ("MATH::FRAC_2_PI", 0.6366197723675814),
    ("MATH::FRAC_2_SQRT_PI", 1.1283791670955126),
    ("MATH::FRAC_PI_2", 1.5707963267948966),
    ("MATH::FRAC_PI_3", 1.0471975511965979),
    ("MATH::FRAC_PI_4", 0.7853981633974483),
    ("MATH::FRAC_PI_6", 0.5235987755982989),
    ("MATH::FRAC_PI_8", 0.39269908169872414),
    ("MATH::LN_10", 2.302585092994046),
    ("MATH::LN_2", 0.6931471805599453),
    ("MATH::LOG10_2", 0.3010299956639812),
    ("MATH::LOG10_E", 0.4342944819032518),
    ("MATH::LOG2_E", 1.4426950408889634),
    ("MATH::LOG2_10", 3.321928094887362),
    ("MATH::PI", 3.141592653589793),
    ("MATH::SQRT_2", 1.4142135623730951),
    ("MATH::TAU", 6.283185307179586),
]


class TestInnerFunctions(TestCase):
    def test_functions(self):
        with Surreal(URL, credentials=('root', 'root')).connect() as conn:
            conn.use("test", "test")
            for func, params, expected in names:
                with self.subTest(f"function {func}({params})"):
                    query = f"RETURN {func}({params});"
                    res = conn.query(query)
                    self.assertFalse(res.is_error(), res)
                    if isinstance(expected, float):
                        self.assertAlmostEqual(float(expected), float(res.result), places=5)
                    else:
                        self.assertEqual(str(expected), str(res.result), res)

    def test_just_works_functions(self):
        with Surreal(URL, credentials=('root', 'root')).connect() as conn:
            for func, params in functions:
                with self.subTest(f"function {func}({params})"):
                    query = f"RETURN {func}({params});"
                    res = conn.query(query)
                    self.assertFalse(res.is_error(), res)

    def test_just_works_variables(self):
        with Surreal(URL, credentials=('root', 'root'), use_http=True).connect() as conn:
            conn.use("test", "test")
            for var in variables:
                with self.subTest(f"variable {var}"):
                    query = f"RETURN {var};"
                    res = conn.query(query)
                    self.assertFalse(res.is_error(), res)

    def test_constants(self):
        with Surreal(URL, credentials=('root', 'root'), use_http=True).connect() as conn:
            for const, expected in constants:
                with self.subTest(f"constant {const}"):
                    query = f"RETURN {const};"
                    res = conn.query(query)
                    self.assertFalse(res.is_error(), res)
                    self.assertEqual(res.result, expected)

    def test_version(self):
        surreal = Surreal("http://127.0.0.1:8000", 'test', 'test', credentials=('root', 'root'))
        version = surreal.version()
        self.assertTrue("2.0." in version, version)


if __name__ == '__main__':
    main()
