--
-- NULL_CHARACTER
-- Test the polar_skip_null_char_in_string GUC parameter
--
\pset null '(null)'
\set prevdb :DBNAME

-- Create test tables
CREATE TABLE null_char_test (
    id serial PRIMARY KEY,
    txt text,
    description varchar(100)
);

--
-- Test 1: GUC off - null characters should cause errors
--
SET polar_skip_null_char_in_string = off;
SHOW polar_skip_null_char_in_string;

-- These should all fail with encoding errors
INSERT INTO null_char_test (txt, description) VALUES (E'abc\000def', 'embedded null');
INSERT INTO null_char_test (txt, description) VALUES (E'Hello\x00World', 'hex null');
INSERT INTO null_char_test (txt, description) VALUES (E'\000start', 'null at start');
INSERT INTO null_char_test (txt, description) VALUES (E'end\000', 'null at end');
INSERT INTO null_char_test (txt, description) VALUES (E'\000\000\000', 'all nulls');
INSERT INTO null_char_test (txt, description) VALUES (E'中文\000测试', 'chinese with null');

-- Verify table is still empty
SELECT count(*) FROM null_char_test;

--
-- Test 2: Enable GUC - null characters should be stripped silently
--
SET polar_skip_null_char_in_string = on;
SHOW polar_skip_null_char_in_string;

-- Test various positions of null bytes
INSERT INTO null_char_test (txt, description) VALUES (E'abc\000def', 'embedded null');
INSERT INTO null_char_test (txt, description) VALUES (E'Hello\x00World', 'hex null');
INSERT INTO null_char_test (txt, description) VALUES (E'\000start', 'null at start');
INSERT INTO null_char_test (txt, description) VALUES (E'end\000', 'null at end');
INSERT INTO null_char_test (txt, description) VALUES (E'mid\000dle\x00test', 'multiple nulls');

-- Test all null bytes (should result in empty string)
INSERT INTO null_char_test (txt, description) VALUES (E'\000', 'single null');
INSERT INTO null_char_test (txt, description) VALUES (E'\000\000\000', 'all nulls');
INSERT INTO null_char_test (txt, description) VALUES (E'\x00\x00\x00\x00', 'all hex nulls');

-- Test with actual NULL value (should remain NULL, not empty string)
INSERT INTO null_char_test (txt, description) VALUES (NULL, 'SQL NULL');

-- Test empty string (should remain empty)
INSERT INTO null_char_test (txt, description) VALUES ('', 'empty string');

-- Test Chinese characters with null bytes
INSERT INTO null_char_test (txt, description) VALUES (E'你好\000世界', 'chinese greeting');
INSERT INTO null_char_test (txt, description) VALUES (E'\000中文测试\000', 'chinese with nulls');
INSERT INTO null_char_test (txt, description) VALUES (E'开头\000中间\000结尾', 'chinese multiple nulls');

-- Test with varchar type
INSERT INTO null_char_test (txt, description) VALUES (E'text field', E'varchar\000test');

-- Verify results: null bytes should be stripped
SELECT id, txt, description, length(txt) as txt_len, length(description) as desc_len 
FROM null_char_test 
ORDER BY id;

-- Verify specific cases
SELECT id, txt, txt IS NULL as is_null, length(txt) as len 
FROM null_char_test 
WHERE description IN ('single null', 'all nulls', 'SQL NULL', 'empty string')
ORDER BY id;

--
-- Test 4: PreparedStatement simulation (parameters via EXECUTE)
--
PREPARE insert_stmt (text, text) AS 
    INSERT INTO null_char_test (txt, description) VALUES ($1, $2) RETURNING id, txt, length(txt);

-- These go through the bind parameter path
EXECUTE insert_stmt(E'param\000test', 'prepared stmt');
EXECUTE insert_stmt(E'\000\000param\000\000', 'prepared multiple nulls');

--
-- Test 5: Disable GUC again - should revert to error behavior
--
SET polar_skip_null_char_in_string = off;
SHOW polar_skip_null_char_in_string;

-- This should fail again
INSERT INTO null_char_test (txt, description) VALUES (E'abc\000def', 'should fail');

-- But data without null bytes should still work
INSERT INTO null_char_test (txt, description) VALUES ('normal text', 'no null bytes');

SELECT txt FROM null_char_test WHERE description = 'no null bytes';

--
-- Test 6: Transaction rollback test
--
SET polar_skip_null_char_in_string = on;

BEGIN;
INSERT INTO null_char_test (txt, description) VALUES (E'transaction\000test', 'in transaction');
SELECT count(*) FROM null_char_test WHERE description = 'in transaction';
ROLLBACK;

-- Should be 0 after rollback
SELECT count(*) FROM null_char_test WHERE description = 'in transaction';

--
-- Test 7: Edge cases
--
SET polar_skip_null_char_in_string = on;

-- Very long string with null bytes
INSERT INTO null_char_test (txt, description) 
VALUES (E'a\000b\000c\000d\000e\000f\000g\000h\000i\000j\000k\000l\000m\000n\000o\000p\000q\000r\000s\000t\000u\000v\000w\000x\000y\000z', 'alphabet nulls');

SELECT txt, length(txt) FROM null_char_test WHERE description = 'alphabet nulls';

-- Mixed escapes
INSERT INTO null_char_test (txt, description) VALUES (E'tab\there\nnewline\000null', 'mixed escapes');

SELECT txt, length(txt) FROM null_char_test WHERE description = 'mixed escapes';

--
-- Test 8: Text operations on stripped data
--
SET polar_skip_null_char_in_string = on;

CREATE TEMP TABLE text_ops_test (original text, expected text);

INSERT INTO text_ops_test VALUES (E'Hello\000World', 'HelloWorld');
INSERT INTO text_ops_test VALUES (E'\000test', 'test');
INSERT INTO text_ops_test VALUES (E'test\000', 'test');

-- Verify string operations work correctly
SELECT 
    original = expected as matches,
    length(original) as orig_len,
    length(expected) as exp_len,
    original,
    expected
FROM text_ops_test;

-- Test concatenation
SELECT E'Hello\000' || E'\000World' as concat_result, length(E'Hello\000' || E'\000World') as len;

-- Test substring
SELECT substring(E'Hello\000World\000Test' from 1 for 20) as substr_result;

-- Test LIKE with stripped nulls
SELECT E'test\000data' LIKE 'testdata' as like_match;

--
-- Test 9: Client encoding with null bytes (simple cases)
--
SET polar_skip_null_char_in_string = on;

-- Test with SQL_ASCII (no conversion)
SET client_encoding = 'SQL_ASCII';
SHOW client_encoding;

CREATE TEMP TABLE sql_ascii_test (id int, data text);

INSERT INTO sql_ascii_test VALUES (1, E'test\000data');
INSERT INTO sql_ascii_test VALUES (2, E'\x00\x00\x00');
INSERT INTO sql_ascii_test VALUES (3, E'\x00start\x00end\x00');

SELECT id, data, length(data) as len FROM sql_ascii_test ORDER BY id;

DROP TABLE sql_ascii_test;

-- Test with UTF8 client encoding (same as server, no conversion)
RESET client_encoding;
SHOW client_encoding;

CREATE TEMP TABLE utf8_test (id int, data text);

INSERT INTO utf8_test VALUES (1, E'test\000data');
INSERT INTO utf8_test VALUES (2, E'中文\x00测试');  -- Chinese with null
INSERT INTO utf8_test VALUES (3, E'\x00UTF8\x00test\x00');

SELECT id, data, length(data) as len FROM utf8_test ORDER BY id;

DROP TABLE utf8_test;

-- Note: We don't test encoding conversion with embedded nulls (like GBK -> UTF8)
-- because it's ambiguous whether nulls should be stripped before or after conversion,
-- and inserting nulls in the middle of multi-byte characters is not a valid use case.


--
-- Test 10: Different database encodings with null bytes
--
SET polar_skip_null_char_in_string = on;

-- Test with SQL_ASCII database encoding
CREATE DATABASE test_sql_ascii_db ENCODING 'SQL_ASCII' LC_COLLATE 'C' LC_CTYPE 'C' TEMPLATE template0;
\c test_sql_ascii_db postgres

SHOW server_encoding;
show client_encoding;
SET polar_skip_null_char_in_string = on;

CREATE TABLE ascii_null_test (id serial PRIMARY KEY, data text, description varchar(100));

-- Test null bytes in SQL_ASCII database
INSERT INTO ascii_null_test (data, description) VALUES (E'abc\000def', 'embedded null');
INSERT INTO ascii_null_test (data, description) VALUES (E'\000start', 'null at start');
INSERT INTO ascii_null_test (data, description) VALUES (E'end\000', 'null at end');
INSERT INTO ascii_null_test (data, description) VALUES (E'\000\000\000', 'all nulls');

SELECT id, data, description, length(data) as len FROM ascii_null_test ORDER BY id;

DROP TABLE ascii_null_test;

-- Test EUC_CN database (Simplified Chinese encoding)
\c :prevdb

CREATE DATABASE test_gbk_db WITH ENCODING 'GBK' LC_COLLATE='zh_CN.GBK' LC_CTYPE='zh_CN.GBK' TEMPLATE=template0;
\c test_gbk_db

SHOW server_encoding;
show client_encoding;
set client_encoding to UTF8;
SET polar_skip_null_char_in_string = on;

CREATE TABLE gbk_client_test (id serial PRIMARY KEY, data text, description varchar(100));

INSERT INTO gbk_client_test (data, description) VALUES (E'abc\000def', 'ASCII with null');
INSERT INTO gbk_client_test (data, description) VALUES (E'\x00test', 'null at start');
INSERT INTO gbk_client_test (data, description) VALUES (E'test\x00', 'null at end');
INSERT INTO gbk_client_test (data, description) VALUES (E'\xD6\xD0\xCE\xC4', 'GBK: 中文 (no null)');
INSERT INTO gbk_client_test (data, description) VALUES (E'\x00\xD6\xD0\xCE\xC4', 'null before GBK: 中文');
INSERT INTO gbk_client_test (data, description) VALUES (E'\xD6\xD0\xCE\xC4\x00', 'GBK: 中文 then null');
INSERT INTO gbk_client_test (data, description) VALUES (E'\xD6\xD0\x00\xCE\xC4', 'GBK: 中 + null + 文');
-- error
INSERT INTO gbk_client_test (data, description) VALUES (E'test\x00\xD6\xD0\xCE\xC4\x00end', 'Mixed: test+null+中文+null+end');
-- ok
INSERT INTO gbk_client_test (data, description)
VALUES (
    E'test\000'      -- avoid \x00
    || '中文'
    || E'\000end',
    'Mixed: test+null+中文+null+end'
);
INSERT INTO gbk_client_test (data, description) VALUES (E'\x00\x00\x00', 'only nulls');

SELECT id, data, description, length(data) as char_len, octet_length(data) as byte_len 
FROM gbk_client_test ORDER BY id;
DROP TABLE gbk_client_test;

--
-- Cleanup
--
-- Cleanup test databases
\c :prevdb
DROP DATABASE test_sql_ascii_db;
DROP DATABASE test_gbk_db;

DROP TABLE null_char_test;
DROP TABLE text_ops_test;

\! rm -f /tmp/null_char_test.dat /tmp/test_gbk_null_utf8.dat
