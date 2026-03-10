CREATE TABLE dccheck_hash_heap (keycol INT);
CREATE INDEX dccheck_hash_index on dccheck_hash_heap USING HASH (keycol);
