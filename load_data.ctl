options (direct-true)
LOAD DATA
INFILE 'datafile.csv'
INTO TABLE tst_load
APPEND
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
TRAILING NULLCOLS
(
  name    CHAR(32),
  payload CHAR(128)
)
