--#DATABASE=alter_database
--#SLEEP=100
DROP DATABASE IF EXISTS alter_database;
ALTER DATABASE alter_database Set TTL '30d';

CREATE DATABASE alter_database WITH TTl '10d' SHARD 5 VNOdE_DURATiON '3d' REPLICA 1 pRECISIOn 'us';

ALTER DATABASE alter_database Set TTL '30d' SHARD 6;

DESCRIBE DATABASE alter_database;

ALTER DATABASE alter_database Set TTL '30d';

DESCRIBE DATABASE alter_database;

ALTER DATABASE alter_database Set SHARD 6;

DESCRIBE DATABASE alter_database;

ALTER DATABASE alter_database Set VNODE_DURATION '100d';

DESCRIBE DATABASE alter_database;

ALTER DATABASE alter_database Set REPLICA 1;

DESCRIBE DATABASE alter_database;

ALTER DATABASE alter_database Set PRECision 'ms';

DESCRIBE DATABASE alter_database;
