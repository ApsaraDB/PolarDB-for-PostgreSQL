--PRIMARY KEY/UNIQUE INDEX 
--For hash or modulo distributed table, first column of the primary must be same as distribution column --?? - needs to be updated

-- default is to distribute to all nodes 

-- this condition (distribution by second column of PRIMARY KEY ) is not erroneous. It is supported.
 
CREATE TABLE xl_pk_products ( product_no integer, product_uid integer, name text, price numeric, primary key (product_uid, product_no) ) with(dist_type=hash, dist_col=product_no);

CREATE TABLE xl_pk_products1 ( product_no integer, product_uid integer, name text, price numeric, primary key (product_uid, product_no) ) with(dist_type=hash, dist_col=product_uid);

CREATE TABLE xl_pk_products2 ( product_no integer, product_uid integer, name text, price numeric, primary key (product_uid) ) with(dist_type=hash, dist_col=product_no); 

CREATE TABLE xl_pk_products3 ( product_no integer, product_uid integer, name text, price numeric, primary key (product_no, product_uid) ) with(dist_type=modulo, dist_col=product_no);

CREATE TABLE xl_pk_products4 ( product_no integer, product_uid integer, name text, price numeric, primary key (product_uid, product_no) ) with(dist_type=modulo, dist_col=product_no);

CREATE TABLE xl_pk_products5 ( product_no integer, product_uid integer, name text, price numeric, primary key (product_uid) ) with(dist_type=modulo, dist_col=product_no);

--For roundrobin distributed table, PRIMARY KEY or UNIQUE INDEX is not supported

CREATE TABLE xl_pk_products6 ( product_no integer, product_uid integer, name text, price numeric, primary key (product_uid) ) with(dist_type=roundrobin);

CREATE TABLE xl_pk_products7 ( product_no integer, product_uid integer, name text, price numeric ) with(dist_type=roundrobin);

CREATE UNIQUE INDEX product_uuid ON xl_pk_products7 (product_uid, product_no); 

DROP TABLE xl_pk_products; 
DROP TABLE xl_pk_products1; 
DROP TABLE xl_pk_products2; 
DROP TABLE xl_pk_products3; 
DROP TABLE xl_pk_products4; 
DROP TABLE xl_pk_products5; 
DROP TABLE xl_pk_products6; 
DROP TABLE xl_pk_products7;
