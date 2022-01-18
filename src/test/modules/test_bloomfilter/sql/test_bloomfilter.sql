CREATE EXTENSION test_bloomfilter;

-- See README for explanation of arguments:
SELECT test_bloomfilter(power => 23,
    nelements => 838861,
    seed => -1,
    tests => 1);

SELECT test_bloomfilter_buf(nelements => 1024,
	seed => -1,
	tests => 1);

-- Equivalent "10 bits per element" tests for all possible bitset sizes:
--
-- SELECT test_bloomfilter(24, 1677722)
-- SELECT test_bloomfilter(25, 3355443)
-- SELECT test_bloomfilter(26, 6710886)
-- SELECT test_bloomfilter(27, 13421773)
-- SELECT test_bloomfilter(28, 26843546)
-- SELECT test_bloomfilter(29, 53687091)
-- SELECT test_bloomfilter(30, 107374182)
-- SELECT test_bloomfilter(31, 214748365)
-- SELECT test_bloomfilter(32, 429496730)
