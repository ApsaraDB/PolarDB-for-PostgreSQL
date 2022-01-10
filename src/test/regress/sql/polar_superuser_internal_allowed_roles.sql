-- test case for polar_internal_allowed_roles
CREATE ROLE polaruser_test_copy LOGIN POLAR_SUPERUSER;
SET SESSION AUTHORIZATION 'polaruser_test_copy';

CREATE TEMP TABLE t
(
  a INT
);

-- error
COPY t FROM PROGRAM 'cat no_such_file';

-- error
COPY t FROM 'log/no_such_file';

DROP TABLE t;

RESET SESSION AUTHORIZATION;
DROP ROLE polaruser_test_copy;