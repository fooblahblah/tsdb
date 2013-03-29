CREATE TABLE TIMESERIES (
   metric VARCHAR(256) NOT NULL,
   time BIGINT NOT NULL,
   value FLOAT NOT NULL,
   PRIMARY KEY (metric, time)
);

PARTITION TABLE TIMESERIES ON COLUMN METRIC;

CREATE PROCEDURE FROM CLASS voltdb.procedures.Delete;
CREATE PROCEDURE FROM CLASS voltdb.procedures.Find;
CREATE PROCEDURE FROM CLASS voltdb.procedures.Upsert;

PARTITION PROCEDURE Find ON TABLE timeseries COLUMN metric;
PARTITION PROCEDURE Upsert ON TABLE timeseries COLUMN metric;
