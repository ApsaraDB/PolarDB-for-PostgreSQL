CREATE EXTENSION test_dsa;

SELECT test_dsa_random(3, 5, 1024, 4096, 'random');
SELECT test_dsa_random(3, 5, 1024, 4096, 'forwards');
SELECT test_dsa_random(3, 5, 1024, 4096, 'backwards');

SELECT count(*) from test_dsa_random_parallel(3, 5, 1024, 8192, 'random', 5);
SELECT count(*) from test_dsa_random_parallel(3, 5, 1024, 8192, 'forwards', 5);
SELECT count(*) from test_dsa_random_parallel(3, 5, 1024, 8192, 'backwards', 5);

SELECT test_dsa_oom();