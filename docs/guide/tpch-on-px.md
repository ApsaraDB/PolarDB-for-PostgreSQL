# Practice：Accelerate TPC-H through PolarDB HTAP

In this section，we will accelerate TPC-H through PolarDB HTAP. The practice will be based on local storage.

## Prepare

### Deploy PolarDB PG on local storage

Before this section, we should deploy PolarDB for PG HTAP through the documents [deploy PolarDB PG on local storage](deploy-on-local-storage.md). There are one primary node(running on port 5432) and two read-only node(running on port 5433/5434).

We can verify PolarDB HTAP through commands as follows：

```shell
ps xf
```

Three processes should exist. There are one primary node(running on port 5432) and two read-only node(running on port 5433/5434).

![HTAP processes](../imgs/64_htap_process_test.png)

### Generate TPC-H Dataset

[TPC-H](https://www.tpc.org/tpch/default5.asp) is a dataset dedicated for OLAP. There are 22 sqls in TPC-H. We will utilize tpch-dbgen to generate arbitrarily sized TPC-H dataset.

```shell
# download tpch-dbgen
git clone https://github.com/qiuyuhang/tpch-dbgen.git

# compile
cd tpch-dbgen
make
```

Next, let's generate some simulation data.

::: tip
It is recommended to follow this command and start with 10GB data. After experiencing this case, you can also try 100GB of data by replacing 10 with 100 in this command. What's more, you should be careful not to exceed the local external storage capacity.
:::

```shell
# Generate simulation data
./dbgen -s 10
```

Let me briefly explain the files inside tpch-dbgen. The .tbl files indicates the generated table data. There are 22 TPC-H sqls in queries/. The explain files only print the plan but not actually execute.

## Load Data

Load TPC-H data with psql

::: tip
Note that it should always be executed in the tpch-dbgen/ directory.
:::

```shell
# 创建表
psql -f dss.ddl

# 进入数据库Cli模式
psql
```

```sql
# 导入数据
\copy nation from 'nation.tbl' DELIMITER '|';
\copy region from 'region.tbl' DELIMITER '|';
\copy supplier from 'supplier.tbl' DELIMITER '|';
\copy part from 'part.tbl' DELIMITER '|';
\copy partsupp from 'partsupp.tbl' DELIMITER '|';
\copy customer from 'customer.tbl' DELIMITER '|';
\copy orders from 'orders.tbl' DELIMITER '|';
\copy lineitem from 'lineitem.tbl' DELIMITER '|';

```

After loading data, execute the following commands to set the maximum parallelism for the created tables.

```sql
# Set maximum parallelism for tables that require PX queries (if not set, no PX queries will be executed)
alter table nation set (px_workers = 100);
alter table region set (px_workers = 100);
alter table supplier set (px_workers = 100);
alter table part set (px_workers = 100);
alter table partsupp set (px_workers = 100);
alter table customer set (px_workers = 100);
alter table orders set (px_workers = 100);
alter table lineitem set (px_workers = 100);
```

## Execute parallel queries in single server

After loading the simulated data, we first execute parallel queries in single server to observe the query speed.

1. After entering psql, execute the following command to turn on the timer.

   ```sql
   \timing
   ```

2. Set max workers in single server through `max_parallel_workers_per_gather`:

   ```sql
   set max_parallel_workers_per_gather=2; -- Set 2
   ```

3. Execute the following command to view the execution plan.

   ```sql
   \i queries/q18.explain.sql
   ```

   You can see the parallel plan in single machine with 2 workers as shown in the figure

   ```sql
                                                                           QUERY PLAN
   -----------------------------------------------------------------------------------------------------------------------------------------------------------------
   Limit  (cost=9364138.51..9364141.51 rows=100 width=71)
   ->  GroupAggregate  (cost=9364138.51..9380736.94 rows=553281 width=71)
         Group Key: orders.o_totalprice, orders.o_orderdate, customer.c_name, customer.c_custkey, orders.o_orderkey
         ->  Sort  (cost=9364138.51..9365521.71 rows=553281 width=44)
               Sort Key: orders.o_totalprice DESC, orders.o_orderdate, customer.c_name, customer.c_custkey, orders.o_orderkey
               ->  Hash Join  (cost=6752588.87..9294341.50 rows=553281 width=44)
                     Hash Cond: (lineitem.l_orderkey = orders.o_orderkey)
                     ->  Seq Scan on lineitem  (cost=0.00..1724338.96 rows=59979696 width=9)
                     ->  Hash  (cost=6749642.22..6749642.22 rows=138372 width=43)
                           ->  Hash Join  (cost=6110531.76..6749642.22 rows=138372 width=43)
                                 Hash Cond: (orders.o_custkey = customer.c_custkey)
                                 ->  Hash Join  (cost=6032162.96..6658785.84 rows=138372 width=24)
                                       Hash Cond: (orders.o_orderkey = lineitem_1.l_orderkey)
                                       ->  Seq Scan on orders  (cost=0.00..410917.44 rows=15000544 width=20)
                                       ->  Hash  (cost=6029892.31..6029892.31 rows=138372 width=4)
                                             ->  Finalize GroupAggregate  (cost=5727599.96..6028508.59 rows=138372 width=4)
                                                   Group Key: lineitem_1.l_orderkey
                                                   Filter: (sum(lineitem_1.l_quantity) > '313'::numeric)
                                                   ->  Gather Merge  (cost=5727599.96..6016055.08 rows=830234 width=36)
                                                         Workers Planned: 2
                                                         ->  Partial GroupAggregate  (cost=5726599.94..5919225.45 rows=415117 width=36)
                                                               Group Key: lineitem_1.l_orderkey
                                                               ->  Sort  (cost=5726599.94..5789078.79 rows=24991540 width=9)
                                                                     Sort Key: lineitem_1.l_orderkey
                                                                     ->  Parallel Seq Scan on lineitem lineitem_1  (cost=0.00..1374457.40 rows=24991540 width=9)
                                 ->  Hash  (cost=50827.80..50827.80 rows=1500080 width=23)
                                       ->  Seq Scan on customer  (cost=0.00..50827.80 rows=1500080 width=23)
   (27 rows)
   ```

4. Execute the following SQL.

   ```sql
   \i queries/q18.sql
   ```

You can see some of the results (press q not to see all the results) and the running time, and you can see that the running time is 1 minute 23 seconds.

![The parallel plan result for Q18](../imgs/68_htap_single_result.png)

::: tip
Note that the following error message appears because of too many workers on a single machine：pq: could not resize shared memory segment "/PostgreSQL.2058389254" to 12615680 bytes: No space left on device.

The reason for that is the default shared memory of docker is not enough. You can refer to [this link](https://stackoverflow.com/questions/56751565/pq-could-not-resize-shared-memory-segment-no-space-left-on-device) and set the parameters to restart docker to fix it.
:::

## Execute parallel queries with PolarDB HTAP

After experiencing single-computer parallel query, we turn on parallel queries in distributed servers.

1. After psql, execute the following command to turn on the timing (if it is already on, you can skip it).

   ```sql
   \timing
   ```

2. Execute the following command to enable parallel query in distributed servers(PX).

   ```sql
   set polar_enable_px=on;
   ```

3. Set workers in every server as 1.

   ```sql
   set polar_px_dop_per_node=1;
   ```

4. Execute the following command to view the execution plan.

   ```sql
   \i queries/q18.explain.sql
   ```

   This cluster comes with 2 ROs and the default workers is 2x1=2 when PX is turned on.

   ```sql
                                                                                              QUERY PLAN
   -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

   Limit  (cost=0.00..93628.34 rows=100 width=47)
   ->  PX Coordinator 2:1  (slice1; segments: 2)  (cost=0.00..93628.33 rows=100 width=47)
         Merge Key: orders.o_totalprice, orders.o_orderdate
         ->  Limit  (cost=0.00..93628.31 rows=50 width=47)
               ->  GroupAggregate  (cost=0.00..93628.31 rows=11995940 width=47)
                     Group Key: orders.o_totalprice, orders.o_orderdate, customer.c_name, customer.c_custkey, orders.o_orderkey
                     ->  Sort  (cost=0.00..92784.19 rows=11995940 width=44)
                           Sort Key: orders.o_totalprice DESC, orders.o_orderdate, customer.c_name, customer.c_custkey, orders.o_orderkey
                           ->  Hash Join  (cost=0.00..22406.63 rows=11995940 width=44)
                                 Hash Cond: (lineitem.l_orderkey = orders.o_orderkey)
                                 ->  PX Hash 2:2  (slice2; segments: 2)  (cost=0.00..4301.49 rows=29989848 width=9)
                                       Hash Key: lineitem.l_orderkey
                                       ->  Partial Seq Scan on lineitem  (cost=0.00..2954.65 rows=29989848 width=9)
                                 ->  Hash  (cost=10799.35..10799.35 rows=83024 width=39)
                                       ->  PX Hash 2:2  (slice3; segments: 2)  (cost=0.00..10799.35 rows=83024 width=39)
                                             Hash Key: orders.o_orderkey
                                             ->  Hash Join  (cost=0.00..10789.21 rows=83024 width=39)
                                                   Hash Cond: (customer.c_custkey = orders.o_custkey)
                                                   ->  PX Hash 2:2  (slice4; segments: 2)  (cost=0.00..597.52 rows=750040 width=23)
                                                         Hash Key: customer.c_custkey
                                                         ->  Partial Seq Scan on customer  (cost=0.00..511.44 rows=750040 width=23)
                                                   ->  Hash  (cost=9993.50..9993.50 rows=83024 width=20)
                                                         ->  PX Hash 2:2  (slice5; segments: 2)  (cost=0.00..9993.50 rows=83024 width=20)
                                                               Hash Key: orders.o_custkey
                                                               ->  Hash Semi Join  (cost=0.00..9988.30 rows=83024 width=20)
                                                                     Hash Cond: (orders.o_orderkey = lineitem_1.l_orderkey)
                                                                     ->  Partial Seq Scan on orders  (cost=0.00..1020.90 rows=7500272 width=20)
                                                                     ->  Hash  (cost=7256.00..7256.00 rows=166047 width=4)
                                                                           ->  PX Broadcast 2:2  (slice6; segments: 2)  (cost=0.00..7256.00 rows=166047 width=4)
                                                                                 ->  Result  (cost=0.00..7238.62 rows=83024 width=4)
                                                                                       Filter: ((sum(lineitem_1.l_quantity)) > '313'::numeric)
                                                                                       ->  Finalize HashAggregate  (cost=0.00..7231.79 rows=207559 width=12)
                                                                                             Group Key: lineitem_1.l_orderkey
                                                                                             ->  PX Hash 2:2  (slice7; segments: 2)  (cost=0.00..7205.20 rows=207559 width=12)
                                                                                                   Hash Key: lineitem_1.l_orderkey
                                                                                                   ->  Partial HashAggregate  (cost=0.00..7197.41 rows=207559 width=12)
                                                                                                         Group Key: lineitem_1.l_orderkey
                                                                                                         ->  Partial Seq Scan on lineitem lineitem_1  (cost=0.00..2954.65 rows=29989848 width=9)
   Optimizer: PolarDB PX Optimizer
   (39 rows)
   ```

5. Execute the following SQL.

   ```sql
   \i queries/q18.sql
   ```

You can see some of the results (press q not to see all the results) and the running time, and you can see that the running time is 1 minute. This is a 27.71\% reduction in runtime compared to the results of single-computer parallelism.
If you are interested, you can also increase the parallelism or the amount of data to see improvement.

![The PX results for q18](../imgs/69_htap_px_result.png)

The PX parallel query goes to get the global consistency view, so the data obtained is consistent and there is no need to worry about data correctness.

We can manually set the workers for PX：

```sql
set polar_px_dop_per_node = 1;
\i queries/q18.sql

set polar_px_dop_per_node = 2;
\i queries/q18.sql

set polar_px_dop_per_node = 4;
\i queries/q18.sql
```
