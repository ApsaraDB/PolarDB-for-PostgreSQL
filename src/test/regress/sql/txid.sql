-- txid_snapshot data type and related functions

-- i/o
select '12:0/ABCDABCD'::txid_snapshot;

-- errors
select '0:0/ABCDABCD'::txid_snapshot;

create temp table snapshot_test (
	nr	integer,
	snap	txid_snapshot
);

insert into snapshot_test values (1, '12:0/ABCDABCD');
select snap from snapshot_test order by nr;

select  txid_snapshot_xmax(snap)
from snapshot_test order by nr;
/*
select id, txid_visible_in_snapshot(id, snap)
from snapshot_test, generate_series(11, 21) id
where nr = 2;

-- test bsearch
select id, txid_visible_in_snapshot(id, snap)
from snapshot_test, generate_series(90, 160) id
where nr = 4;
*/
-- test current values also
select txid_current() >= txid_snapshot_xmin(txid_current_snapshot());

-- we can't assume current is always less than xmax, however

select txid_visible_in_snapshot(txid_current(), txid_current_snapshot());

/*
-- test 64bitness

select txid_snapshot '1000100010001000:1000100010001100:1000100010001012,1000100010001013';
select txid_visible_in_snapshot('1000100010001012', '1000100010001000:1000100010001100:1000100010001012,1000100010001013');
select txid_visible_in_snapshot('1000100010001015', '1000100010001000:1000100010001100:1000100010001012,1000100010001013');

-- test 64bit overflow
SELECT txid_snapshot '1:9223372036854775807:3';
SELECT txid_snapshot '1:9223372036854775808:3';

-- test txid_current_if_assigned
BEGIN;
SELECT txid_current_if_assigned() IS NULL;
SELECT txid_current() \gset
SELECT txid_current_if_assigned() IS NOT DISTINCT FROM BIGINT :'txid_current';
COMMIT;

-- test xid status functions
BEGIN;
SELECT txid_current() AS committed \gset
COMMIT;

BEGIN;
SELECT txid_current() AS rolledback \gset
ROLLBACK;

BEGIN;
SELECT txid_current() AS inprogress \gset

SELECT txid_status(:committed) AS committed;
SELECT txid_status(:rolledback) AS rolledback;
SELECT txid_status(:inprogress) AS inprogress;
SELECT txid_status(1); -- BootstrapTransactionId is always committed
SELECT txid_status(2); -- FrozenTransactionId is always committed
SELECT txid_status(3); -- in regress testing FirstNormalTransactionId will always be behind oldestXmin

COMMIT;

BEGIN;
CREATE FUNCTION test_future_xid_status(bigint)
RETURNS void
LANGUAGE plpgsql
AS
$$
BEGIN
  PERFORM txid_status($1);
  RAISE EXCEPTION 'didn''t ERROR at xid in the future as expected';
EXCEPTION
  WHEN invalid_parameter_value THEN
	RAISE NOTICE 'Got expected error for xid in the future';
END;
$$;
SELECT test_future_xid_status(:inprogress + 10000);
ROLLBACK;
*/
