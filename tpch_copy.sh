#!/bin/bash

# default configuration
# user: "postgres"
# database: "postgres"
# host: "localhost"
# primary port: "5432"
pg_user=postgres
pg_database=postgres
pg_host=localhost
pg_port=5432
clean=
tpch_dir=tpch-dbgen
data_dir=/data

usage () {
cat <<EOF

  1) Use default configuration to run tpch_copy
  ./tpch_copy.sh
  2) Use limited configuration to run tpch_copy
  ./tpch_copy.sh --user=postgres --db=postgres --host=localhost --port=5432
  3) Clean the test data. This step will drop the database or tables.
  ./tpch_copy.sh --clean

EOF
  exit 0;
}

for arg do
  val=`echo "$arg" | sed -e 's;^--[^=]*=;;'`

  case "$arg" in
    --user=*)                   pg_user="$val";;
    --db=*)                     pg_database="$val";;
    --host=*)                   pg_host="$val";;
    --port=*)                   pg_port="$val";;
    --clean)                    clean=on ;;
    -h|--help)                  usage ;;
    *)                          echo "wrong options : $arg";
                                exit 1
                                ;;
  esac
done

export PGPORT=$pg_port
export PGHOST=$pg_host
export PGDATABASE=$pg_database
export PGUSER=$pg_user

# clean the tpch test data
if [[ $clean == "on" ]];
then
  make clean
  if [[ $pg_database == "postgres" ]];
  then
    echo "drop all the tpch tables"
    psql -c "drop table customer cascade"
    psql -c "drop table lineitem cascade"
    psql -c "drop table nation cascade"
    psql -c "drop table orders cascade"
    psql -c "drop table part cascade"
    psql -c "drop table partsupp cascade"
    psql -c "drop table region cascade"
    psql -c "drop table supplier cascade"
  else
    echo "drop the tpch database: $PGDATABASE"
    psql -c "drop database $PGDATABASE" -d postgres
  fi
  exit;
fi


###################### PHASE 1: create table ######################
if [[ $PGDATABASE != "postgres" ]];
then
  echo "create the tpch database: $PGDATABASE"
  psql -c "create database $PGDATABASE" -d postgres
fi
psql -f $tpch_dir/dss.ddl

###################### PHASE 2: load data ######################
psql -c "\COPY nation FROM '$data_dir/nation.tbl' WITH (FORMAT csv, DELIMITER '|');" &
psql -c "\COPY region FROM '$data_dir/region.tbl' WITH (FORMAT csv, DELIMITER '|');" &
psql -c "\COPY part FROM '$data_dir/part.tbl' WITH (FORMAT csv, DELIMITER '|');" &
psql -c "\COPY supplier FROM '$data_dir/supplier.tbl' WITH (FORMAT csv, DELIMITER '|');" &
psql -c "\COPY partsupp FROM '$data_dir/partsupp.tbl' WITH (FORMAT csv, DELIMITER '|');" &
psql -c "\COPY customer FROM '$data_dir/customer.tbl' WITH (FORMAT csv, DELIMITER '|');" &
psql -c "\COPY orders FROM '$data_dir/orders.tbl' WITH (FORMAT csv, DELIMITER '|');" &
psql -c "\COPY lineitem FROM '$data_dir/lineitem.tbl' WITH (FORMAT csv, DELIMITER '|');" &

###################### PHASE 3: add primary and foreign key ######################
psql -f $tpch_dir/dss.ri

psql -c "alter system set polar_enable_px = on;"
psql -c "alter system SET polar_px_dop_per_node = 1;"
psql -c "alter system set work_mem = '16MB';"
psql -c "alter system set polar_px_optimizer_enable_hashagg=0;"
psql -c "alter system set statement_timeout='10min';"
psql -c "select pg_reload_conf();"

/home/postgres/tmp_basedir_polardb_pg_1100_bld/bin/pg_ctl restart -D /home/postgres/tmp_master_dir_polardb_pg_1100_bld
/home/postgres/tmp_basedir_polardb_pg_1100_bld/bin/pg_ctl restart -D /home/postgres/tmp_replica_dir_polardb_pg_1100_bld1
/home/postgres/tmp_basedir_polardb_pg_1100_bld/bin/pg_ctl restart -D /home/postgres/tmp_replica_dir_polardb_pg_1100_bld2