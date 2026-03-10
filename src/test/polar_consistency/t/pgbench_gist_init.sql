CREATE TABLE dccheck_gist (c1 int, c2 int, c3 int, c4 box);
CREATE INDEX dccheck_gist_idx ON dccheck_gist using gist (c4) INCLUDE (c1,c3);
