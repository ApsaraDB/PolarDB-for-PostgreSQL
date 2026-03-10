--
-- Make sure strings are validated
-- Should fail for all encodings, as nul bytes are never permitted.
--
CREATE OR REPLACE FUNCTION perl_zerob() RETURNS TEXT AS $$
  return "abcd\0efg";
$$ LANGUAGE plperl;

CREATE OR REPLACE FUNCTION perl_zerob2() RETURNS TEXT AS $$
  return "\0";
$$ LANGUAGE plperl;

CREATE OR REPLACE FUNCTION perl_zerob3() RETURNS TEXT AS $$
  return "\0\0\0";
$$ LANGUAGE plperl;

CREATE OR REPLACE FUNCTION perl_zerob4() RETURNS TEXT AS $$
  return "\0a\0b\0";
$$ LANGUAGE plperl;

SET polar_skip_null_char_in_string = off;
SELECT perl_zerob();
SELECT perl_zerob2(), perl_zerob2() IS NULL, length(perl_zerob2());
SELECT perl_zerob3(), perl_zerob3() IS NULL, length(perl_zerob3());
SELECT perl_zerob4(), perl_zerob4() IS NULL, length(perl_zerob4());

SET polar_skip_null_char_in_string = on;
SELECT perl_zerob();
SELECT perl_zerob2(), perl_zerob2() IS NULL, length(perl_zerob2());
SELECT perl_zerob3(), perl_zerob3() IS NULL, length(perl_zerob3());
SELECT perl_zerob4(), perl_zerob4() IS NULL, length(perl_zerob4());

RESET polar_skip_null_char_in_string;
