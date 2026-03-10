--default error
CREATE EXTENSION test_autocascade1;

-- set guc, empty
SET polar_auto_cascade_extensions TO '';

-- error
CREATE EXTENSION test_autocascade1;

-- set guc, single
SET polar_auto_cascade_extensions TO test_autocascade1;

-- SUCCESS
CREATE EXTENSION test_autocascade1;

--Clean Up
DROP EXTENSION IF EXISTS test_autocascade1;
DROP EXTENSION IF EXISTS test_autocascade2;

-- set guc, end
SET polar_auto_cascade_extensions TO test_ext1, test_autocascade1;

-- SUCCESS
CREATE EXTENSION test_autocascade1;

--Clean Up
DROP EXTENSION IF EXISTS test_autocascade1;
DROP EXTENSION IF EXISTS test_autocascade2;

-- set guc, start
SET polar_auto_cascade_extensions TO test_autocascade1, test_ext1;

-- SUCCESS
CREATE EXTENSION test_autocascade1;

--Clean Up
DROP EXTENSION IF EXISTS test_autocascade1;
DROP EXTENSION IF EXISTS test_autocascade2;

-- set guc, middle
SET polar_auto_cascade_extensions TO test_ext1, test_autocascade1, test_ext1;

-- SUCCESS
CREATE EXTENSION test_autocascade1;

--Clean Up
DROP EXTENSION IF EXISTS test_autocascade1;
DROP EXTENSION IF EXISTS test_autocascade2;

RESET client_min_messages;
\unset SHOW_CONTEXT
