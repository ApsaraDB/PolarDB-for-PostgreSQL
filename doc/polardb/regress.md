# Regression Tests

The regression tests are a comprehensive set of tests for the SQL implementation in POLARDB for PostgreSQL. They test standard SQL operations as well as the extended capabilities of POLARDB.

## Running the Tests
The regression tests can be run against an already installed and running server, or using a temporary installation within the build tree. Furthermore, there is a parallel
and sequential mode for running the tests. The sequential method runs each test script alone, while the parallel method starts up multiple server processes to run groups of tests in parallel.
Parallel testing adds confidence that interprocess communication and locking are working correctly. We support single node regression test and X-Paxos based three-nodes cluster regression test using a temporary installation within the build tree.

## Three-nodes Regression With Paxos
### Running the Tests Against a Temporary Installation

	make checkdma

in the top-level directory. (Or you can change to and run the command there.) At the end you should see something like:

	============================================
	All 187 tests passed. 1 tests in dma ignore
	============================================

Because this test method runs a temporary server, it will not work if you did the build as the root user, since the server will not start as root. Recommended procedure is not to do the build as root, or else to perform testing after completing the installation.

The parallel regression test starts quite a few processes under your user ID. Presently, the maximum concurrency is twenty parallel test scripts, which means forty processes: there's a server process and a psql process for each test script. So if your system enforces a per-user limit on the number of processes, make sure this limit is at least fifty or so, else you might get random-seeming failures in the parallel test. If you are not in a position to raise the limit, you can cut down the degree of parallelism by setting the MAX_CONNECTIONS parameter. For example:

	make MAX_CONNECTIONS=10 checkdma

## Additional Test Suites
The make check command run only the “core” regression tests, which test built-in functionality of the PostgreSQL server. The source distribution also contains additional test suites, most of them having to do with add-on functionality such as optional procedural languages.

To run all test suites applicable to the modules that have been selected to be built, including the core tests, type one of these commands at the top of the build tree:

	make check-world-dma

These commands run the tests using temporary servers, just as previously explained for make checkdma. Other considerations are the same as previously explained for each method. Note that make check-world-dma builds a separate temporary installation tree for each tested module, so it requires a great deal more time and disk space.

Alternatively, you can run individual test suites by typing make checkdma in the appropriate subdirectory of the build tree.

The additional tests that can be invoked this way include:

* Regression tests for optional procedural languages (other than PL/pgSQL, which is tested by the core tests). These are located under src/pl.

* Regression tests for contrib modules, located under contrib. Not all contrib modules have tests.

* Regression tests for the ECPG interface library, located in src/interfaces/ecpg/test.

* Tests stressing behavior of concurrent sessions, located in src/test/isolation.

* Tests of client programs under src/bin

The TAP-based tests are run only when PostgreSQL was configured with the option --enable-tap-tests. This is recommended for development, but can be omitted if there is no suitable Perl installation.

## Other Tests
You can refer to Postgresql regression tests.

## Single Node Regression
You can refer to Postgresql regression tests.
