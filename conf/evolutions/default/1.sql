# --- !Ups

CREATE TABLE timeseries (
   metric VARCHAR(256) NOT NULL,
   time BIGINT NOT NULL,
   value DOUBLE NOT NULL,
   PRIMARY KEY (metric, time)
);

# --- !Downs

DROP TABLE timeseries;
