create table test_spi_table(id integer);
insert into test_spi_table values (1),(2),(3),(4),(5),(6);
CREATE OR REPLACE FUNCTION test_spi_select(i integer)
    RETURNS void
AS
$$
begin
EXECUTE 'SELECT * FROM test_spi_table';
end;
$$ LANGUAGE 'plpgsql';
CREATE OR REPLACE FUNCTION test_spi_select_system(i integer)
    RETURNS void
AS
$$
begin
EXECUTE 'SELECT * FROM pg_am LIMIT 1';
end;
$$ LANGUAGE 'plpgsql';
set polar_px_enable_spi_read_all_namespaces=false;
select test_spi_select_system(id) test_spi_select from test_spi_table;
select test_spi_select(id) from test_spi_table;
set polar_px_enable_spi_read_all_namespaces=true;
select test_spi_select_system(id) test_spi_select from test_spi_table;
select test_spi_select(id) from test_spi_table;
drop table test_spi_table;
