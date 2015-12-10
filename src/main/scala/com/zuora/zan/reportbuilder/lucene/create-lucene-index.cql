CREATE TABLE zan.lucene_index (
    dir text,
    id text,
    block_id bigint,
    block blob,
    block_size bigint static,
    check_sum bigint static,
    created_time timestamp static,
    curr_block bigint static,
    length bigint static,
    modified_time timestamp static,
    name text static,
    PRIMARY KEY ((dir, id), block_id)
) WITH CLUSTERING ORDER BY (block_id ASC);