CREATE EXTENSION test_logindex;
SET client_min_messages TO 'error';

CHECKPOINT;
-- See README for explanation of arguments:
BEGIN;
SET TRANSACTION ISOLATION LEVEL SERIALIZABLE, READ ONLY, DEFERRABLE;
SELECT test_fullpage_logindex();
ABORT;

RESET client_min_messages;
