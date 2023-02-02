---
author: digoal
date: 2023/02/02
minute: 15
---

# 采用 iprange 和 SPGiST index 加速全球化业务用户体验 - 根据来源 IP 智能路由就近机房降低访问延迟

<ArticleInfo :frontmatter=$frontmatter></ArticleInfo>

## 背景

上一篇信息: [《如何获得 IP 地址对应的地理信息库, 实现智能 DNS 解析? 就近路由提升全球化部署业务的访问性能》](https://github.com/digoal/blog/blob/master/202211/20221124_09.md) 提到了如何获取 IP 地址段的地理信息库, 本篇信息将使用 PolarDB for PostgreSQL 来加速根据来源 IP 快速找到对应的 IP 地址段, 将用到 PolarDB for PostgreSQL 的 SPGiST 索引和 inet 数据类型.

相比于把 IP 地址段存储为 2 个 int8 字段作 between and 的匹配, SPGiST 索引和 inet 数据类型至少可以提升 20 倍性能.

用到的 inet 数据类型如下:

https://www.postgresql.org/docs/15/functions-net.html

```
inet >>= inet → boolean
Does subnet contain or equal subnet?
inet '192.168.1/24' >>= inet '192.168.1/24' → t
```

## 实例讲解

1、将数据导入 PolarDB for PostgreSQL, 使用 inet 类型存储地址段, 并创建 spgist 索引.

```
create table ip2geo (id serial primary key, ip inet, province text, city text);

copy ip2geo(ip,province,city) from '/Users/digoal/c.csv' (format csv);
COPY 8617

create index idx_1 on ip2geo using spgist (ip);
```

```
postgres=# select ip,host(ip), masklen(ip) from ip2geo limit 10;
     ip      |   host   | masklen
-------------+----------+---------
 1.0.1.0/24  | 1.0.1.0  |      24
 1.0.2.0/23  | 1.0.2.0  |      23
 1.0.8.0/21  | 1.0.8.0  |      21
 1.0.32.0/19 | 1.0.32.0 |      19
 1.1.0.0/24  | 1.1.0.0  |      24
 1.1.2.0/23  | 1.1.2.0  |      23
 1.1.4.0/22  | 1.1.4.0  |      22
 1.1.8.0/24  | 1.1.8.0  |      24
 1.1.9.0/24  | 1.1.9.0  |      24
 1.1.10.0/23 | 1.1.10.0 |      23
(10 rows)
```

2、IP 地址段包含查询例子

```
postgres=# select * FROM ip2geo where  ip >>= '1.88.0.10/32' ;
 id |     ip      | province |   city
----+-------------+----------+----------
 53 | 1.88.0.0/14 | 北京市   | 歌华宽带
(1 row)

postgres=# select * FROM ip2geo where  ip >>= '1.88.0.0/24' ;
 id |     ip      | province |   city
----+-------------+----------+----------
 53 | 1.88.0.0/14 | 北京市   | 歌华宽带
(1 row)
```

3、对比索引扫描的性能提升, 相比全表扫描性能相差 25 倍:

```
postgres=# explain (analyze,verbose,timing,costs,buffers) select * FROM ip2geo where  ip >>= '1.88.0.0/24' ;
                                                      QUERY PLAN
----------------------------------------------------------------------------------------------------------------------
 Index Scan using idx_1 on public.ip2geo  (cost=0.15..2.37 rows=1 width=35) (actual time=0.019..0.020 rows=1 loops=1)
   Output: id, ip, province, city
   Index Cond: (ip2geo.ip >>= '1.88.0.0/24'::inet)
   Buffers: shared hit=4
 Planning Time: 0.057 ms
 Execution Time: 0.031 ms
(6 rows)

postgres=# set enable_indexscan=off;
SET
postgres=# set enable_bitmapscan=off;
SET
postgres=# explain (analyze,verbose,timing,costs,buffers) select * FROM ip2geo where  ip >>= '1.88.0.0/24' ;
                                                QUERY PLAN
----------------------------------------------------------------------------------------------------------
 Seq Scan on public.ip2geo  (cost=0.00..175.71 rows=1 width=35) (actual time=0.013..0.783 rows=1 loops=1)
   Output: id, ip, province, city
   Filter: (ip2geo.ip >>= '1.88.0.0/24'::inet)
   Rows Removed by Filter: 8616
   Buffers: shared hit=68
 Planning Time: 0.056 ms
 Execution Time: 0.793 ms
(7 rows)
```

4、压力测试方法, 随机从地址库中取一条记录并生成这个地址段内的随机 IP 地址.

```
create or replace function getipaddr(int default ceil(8617*random())) returns inet as $$
  select ip + (floor(random()*(2^(32-masklen(ip)))))::int8 from ip2geo where id=$1;
$$ language sql strict immutable;
```

```
postgres=# explain (analyze,verbose,timing,costs,buffers) select * FROM ip2geo where  ip >>=  getipaddr();
                                                QUERY PLAN
----------------------------------------------------------------------------------------------------------
 Seq Scan on public.ip2geo  (cost=0.00..175.71 rows=1 width=35) (actual time=0.098..0.955 rows=1 loops=1)
   Output: id, ip, province, city
   Filter: (ip2geo.ip >>= '43.243.11.49/22'::inet)
   Rows Removed by Filter: 8616
   Buffers: shared hit=68
 Planning:
   Buffers: shared hit=14
 Planning Time: 0.370 ms
 Execution Time: 0.962 ms
(9 rows)

postgres=# explain (analyze,verbose,timing,costs,buffers) select * FROM ip2geo where  ip >>=  getipaddr();
                                                QUERY PLAN
----------------------------------------------------------------------------------------------------------
 Seq Scan on public.ip2geo  (cost=0.00..175.71 rows=1 width=35) (actual time=0.087..1.285 rows=1 loops=1)
   Output: id, ip, province, city
   Filter: (ip2geo.ip >>= '43.236.136.57/22'::inet)
   Rows Removed by Filter: 8616
   Buffers: shared hit=68
 Planning:
   Buffers: shared hit=1
 Planning Time: 0.244 ms
 Execution Time: 1.293 ms
(9 rows)

postgres=# explain (analyze,verbose,timing,costs,buffers) select * FROM ip2geo where  ip >>=  getipaddr();
                                                QUERY PLAN
----------------------------------------------------------------------------------------------------------
 Seq Scan on public.ip2geo  (cost=0.00..175.71 rows=1 width=35) (actual time=0.780..0.890 rows=1 loops=1)
   Output: id, ip, province, city
   Filter: (ip2geo.ip >>= '203.19.72.14/24'::inet)
   Rows Removed by Filter: 8616
   Buffers: shared hit=68
 Planning:
   Buffers: shared hit=1
 Planning Time: 0.199 ms
 Execution Time: 0.899 ms
(9 rows)
```

5、使用 prepared statement, 随机地址段包含匹配查询

```
alter function getipaddr(int) volatile;

create or replace function dyn_pre() returns setof ip2geo as $$
declare
  v inet;
begin
  v := getipaddr();
  return query execute format('execute p(%L)', v);
  exception when others then
    execute format('prepare p(inet) as select * from ip2geo where ip >>= $1');
    return query execute format('execute p(%L)', v);
end;
$$ language plpgsql strict;
```

```
postgres=# select dyn_pre();
               dyn_pre
-------------------------------------
 (43.227.192.0/22,浙江省杭州市,电信)
(1 row)

postgres=# select dyn_pre();
           dyn_pre
------------------------------
 (103.25.64.0/22,上海市,电信)
(1 row)

postgres=# select dyn_pre();
         dyn_pre
-------------------------
 (45.119.232.0/22,中国,)
(1 row)

postgres=# select dyn_pre();
               dyn_pre
--------------------------------------
 (103.205.252.0/22,江苏省宿迁市,联通)
(1 row)

postgres=# select dyn_pre();
         dyn_pre
-------------------------
 (103.87.4.0/22,北京市,)
(1 row)
```

6、压力测试

```
vi test.sql
select dyn_pre();


pgbench -M simple -n -r -P 1 -f ./test.sql -c 12 -j 12 -T 120
```

除去获取随机 IP 的时间, 在 2018 款 macbook pro i5 的机器上, 实际约 8 万 qps.

PolarDB for PostgreSQL 作为智能 DNS 的数据搜索引擎, 节省几十倍的成本, 同时提升终端用户就近访问的体验, 特别适合例如“社交、游戏、多媒体、云盘、多地办公等等全球化或者全国部署业务”.

## 为什么 spgist 索引比 btree combine 2 字段索引范围搜索更高效?

spgist 索引不管搜索什么范围, 搜索到目标数据基本上只需要扫描几个数据块.

而使用 btree, 由于是 2 个字段符合搜索, 必然的会出现数据在驱动列大范围的匹配到后, 再通过第二个字段二次过滤的情况. 扫描的数据更多了, 效率立马就下降了.

测试过程:

1、创建 inet 转 int8 的函数

```
create or replace function inet2int8 (inet) returns int8 as $$
  select (v[1]::bit(8)||v[2]::bit(8)||v[3]::bit(8)||v[4]::bit(8))::bit(32)::int8 from ( values ((regexp_split_to_array(host($1),'\.'))::int[]) ) t (v);
$$ language sql strict;


postgres=# select inet2int8('203.88.32.0');
 inet2int8
------------
 3411550208
(1 row)
```

2、将 ip2geo 拆成 int8 存储

```
create table ip2int8geo (id serial primary key, f int8,t int8, province text, city text);

insert into ip2int8geo (f,t,province,city) select inet2int8(network(ip)), inet2int8(network(ip)) + 2^(32-masklen(ip)) - 1, province, city from ip2geo;
INSERT 0 8617
```

3、创建 from to 两个字段的 combine 索引

```
create index idx_2 on ip2int8geo (f,t);
```

4、创建获取随机 IP INT8 的函数用于测试

```
create or replace function genrandomipint8(int) returns int8 as $$
  select f + ceil((t-f)*random()) from ip2int8geo where id=$1;
$$ language sql strict;

-- 这个是驱动列靠前的值, 搜索较快
postgres=# select genrandomipint8(10);
 genrandomipint8
-----------------
        16845351
(1 row)

-- 这个是驱动列靠后的值, 明显看出btree的大范围过滤
postgres=# select genrandomipint8(8000);
 genrandomipint8
-----------------
      3411550659
(1 row)
```

```
postgres=# explain (analyze,verbose,timing,costs,buffers) select * from ip2int8geo where f <= 16845351 and t >= 16845351;
                                                        QUERY PLAN
--------------------------------------------------------------------------------------------------------------------------
 Index Scan using idx_2 on public.ip2int8geo  (cost=0.29..2.50 rows=1 width=44) (actual time=0.006..0.007 rows=1 loops=1)
   Output: id, f, t, province, city
   Index Cond: ((ip2int8geo.f <= 16845351) AND (ip2int8geo.t >= 16845351))
   Buffers: shared hit=3
 Planning:
   Buffers: shared hit=3
 Planning Time: 0.114 ms
 Execution Time: 0.018 ms
(8 rows)



postgres=# explain (analyze,verbose,timing,costs,buffers) select * from ip2int8geo where f <= 3411550659 and t >= 3411550659;
                                                          QUERY PLAN
------------------------------------------------------------------------------------------------------------------------------
 Index Scan using idx_2 on public.ip2int8geo  (cost=0.29..167.31 rows=568 width=44) (actual time=0.438..0.440 rows=1 loops=1)
   Output: id, f, t, province, city
   Index Cond: ((ip2int8geo.f <= '3411550659'::bigint) AND (ip2int8geo.t >= '3411550659'::bigint))
   Buffers: shared hit=33
 Planning Time: 0.133 ms
 Execution Time: 0.469 ms
(6 rows)

postgres=# select * from ip2int8geo where f <= 3411550659 and t >= 3411550659;
  id  |     f      |     t      |   province   |        city
------+------------+------------+--------------+---------------------
 8000 | 3411550208 | 3411558399 | 广东省深圳市 | 天威有线宽带(关内))
(1 row)
```

5、创建一个函数, 用于作 btree 索引的压力测试

```
create or replace function test_getip2int8geo () returns setof ip2int8geo as $$
declare
  i int8;
begin
  i := genrandomipint8( ceil(random()*8617)::int );
  return query select * from ip2int8geo where f <= i and t >= i;
end;
$$ language plpgsql strict;



postgres=# select * from test_getip2int8geo();
  id  |     f      |     t      | province |  city
------+------------+------------+----------+--------
 3798 | 1736744960 | 1736745983 | 台湾省   | 台北市
(1 row)
Time: 1.058 ms

postgres=# select * from test_getip2int8geo();
  id  |     f     |     t     | province |  city
------+-----------+-----------+----------+--------
 1385 | 771539968 | 771540991 | 北京市   | 鹏博士
(1 row)
Time: 0.615 ms
```

6、使用 spgist 索引, 数据不管靠前还是靠后, 扫描的数据块都差不多, 性能基本都一样.

```
postgres=# select * from ip2geo offset 7999 limit 1;
       ip       |   province   |        city
----------------+--------------+---------------------
 203.88.32.0/19 | 广东省深圳市 | 天威有线宽带(关内))
(1 row)


postgres=# explain (analyze,verbose,timing,costs,buffers) select * FROM ip2geo where  ip >>= '203.88.45.200/19' ;
                                                      QUERY PLAN
----------------------------------------------------------------------------------------------------------------------
 Index Scan using idx_1 on public.ip2geo  (cost=0.15..4.60 rows=3 width=31) (actual time=0.031..0.031 rows=1 loops=1)
   Output: ip, province, city
   Index Cond: (ip2geo.ip >>= '203.88.45.200/19'::inet)
   Buffers: shared hit=4
 Planning Time: 0.066 ms
 Execution Time: 0.046 ms
(6 rows)


postgres=# select * FROM ip2geo where  ip >>= '203.88.45.200/19' ;
       ip       |   province   |        city
----------------+--------------+---------------------
 203.88.32.0/19 | 广东省深圳市 | 天威有线宽带(关内))
(1 row)
```

记录越多, btree combine 扫描过滤性越差, 与 spgist 索引的差距就会越大. 例如, 我们可以使用以下 100 万条测试 case 来证明这个结论.  
扫描的数据块数量相差上百倍.

```
create sequence seq INCREMENT by 1000;

create table test (f int, t int);

insert into test select n , n+999 from (select nextval('seq') n from generate_series(1,1000000) ) t ;

postgres=#  select * from test limit 10;
  f   |   t
------+-------
    1 |  1000
 1001 |  2000
 2001 |  3000
 3001 |  4000
 4001 |  5000
 5001 |  6000
 6001 |  7000
 7001 |  8000
 8001 |  9000
 9001 | 10000
(10 rows)


postgres=# select min(f), max(t) from test;
 min |    max
-----+------------
   1 | 1000000000
(1 row)

create index idx_test on test (f,t);

explain (analyze,verbose,timing,costs,buffers) select * from test where f <= 500000000 and t>=500000000;

postgres=# explain (analyze,verbose,timing,costs,buffers) select * from test where f <= 500000000 and t>=500000000;
                                                             QUERY PLAN
-------------------------------------------------------------------------------------------------------------------------------------
 Index Only Scan using idx_test on public.test  (cost=0.42..9021.81 rows=250000 width=8) (actual time=16.605..16.608 rows=1 loops=1)
   Output: f, t
   Index Cond: ((test.f <= 500000000) AND (test.t >= 500000000))
   Heap Fetches: 0
   Buffers: shared hit=1370
 Planning Time: 0.098 ms
 Execution Time: 16.629 ms
(7 rows)



create index idx_test_1 on test using spgist (int4range(f,t+1));
or
create index idx_test_2 on test using gist (int4range(f,t+1));

explain (analyze,verbose,timing,costs,buffers) select * from test where int4range(f,t+1) @> 500000000;


vi t1.sql
\set id random(1,1000000000)
select * from test where f <= :id and t >= :id;

vi t2.sql
\set id random(1,1000000000)
select * from test where int4range(f,t+1) @> :id;


pgbench -M prepared -n -r -P 1 -f ./t1.sql -c 12 -j 12 -T 120

pgbench -M prepared -n -r -P 1 -f ./t2.sql -c 12 -j 12 -T 120
```

## 参考

[《PostgreSQL 随机查询采样 - 既要真随机、又要高性能 - table sample 方法》](https://github.com/digoal/blog/blob/master/202105/20210527_01.md)

[《如何获得 IP 地址对应的地理信息库, 实现智能 DNS 解析? 就近路由提升全球化部署业务的访问性能》](https://github.com/digoal/blog/blob/master/202211/20221124_09.md)

[《PostgreSQL Oracle 兼容性之 - DBMS_SQL(存储过程动态 SQL 中使用绑定变量-DB 端 prepare statement)》](https://github.com/digoal/blog/blob/master/201803/20180323_02.md)

[《PostgreSQL 黑科技 range 类型及 gist index 20x+ speedup than Mysql index combine query》](https://github.com/digoal/blog/blob/master/201206/20120607_01.md)

[《PostgreSQL 黑科技 range 类型及 gist index 助力物联网(IoT)》](https://github.com/digoal/blog/blob/master/201205/20120517_01.md)

[《PostgreSQL gist, spgist 索引的原理、差别、应用场景》](https://github.com/digoal/blog/blob/master/201906/20190604_03.md)

[《PostgreSQL SP-GiST 索引原理》](https://github.com/digoal/blog/blob/master/202011/20201128_01.md)

[《PostgreSQL 黑科技 - 空间聚集存储, 内窥 GIN, GiST, SP-GiST 索引》](https://github.com/digoal/blog/blob/master/201709/20170905_01.md)

[《自动选择正确索引访问接口(btree,hash,gin,gist,sp-gist,brin,bitmap...)的方法》](https://github.com/digoal/blog/blob/master/201706/20170617_01.md)

[《从难缠的模糊查询聊开 - PostgreSQL 独门绝招之一 GIN , GiST , SP-GiST , RUM 索引原理与技术背景》](https://github.com/digoal/blog/blob/master/201612/20161231_01.md)

https://www.postgresql.org/docs/15/functions-net.html

https://gis.stackexchange.com/questions/374091/when-to-use-gist-and-when-to-use-sp-gist-index
