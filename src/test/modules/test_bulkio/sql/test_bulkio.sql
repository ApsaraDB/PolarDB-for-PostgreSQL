CREATE EXTENSION test_bulkio;

-- The default RELSEG_SIZE is 128GB on this PolarDB version, skip test with
-- polar_zero_extend_method = 'none' or 'bulkwrite'
set polar_zero_extend_method = 'fallocate';
SELECT test_bulkio();
