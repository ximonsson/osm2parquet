CREATE TABLE node AS SELECT * FROM 'nodes.parquet';
CREATE TABLE nodetag AS SELECT * FROM 'node-tags.parquet';

CREATE TABLE way AS SELECT * FROM 'ways.parquet';
CREATE TABLE waytag AS SELECT * FROM 'way-tags.parquet';
CREATE TABLE waynode AS SELECT * FROM 'way-nodes.parquet';

CREATE TABLE rel AS SELECT * FROM 'relations.parquet';
CREATE TABLE reltag AS SELECT * FROM 'relation-tags.parquet';
CREATE TABLE relmem AS SELECT * FROM 'relation-members.parquet';
