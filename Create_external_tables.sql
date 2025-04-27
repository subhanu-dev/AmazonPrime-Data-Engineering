-- set password & create master key
create master key encryption by password = ''
-- to access the external location, this is not the database password


-- databased scoped credential managed identity

-- to autenticate and allow synapse to interact with other azurre services
create database scoped credential subhanu_prime
with 
    identity = 'managed identity';

-- create external data source connection between synapse and ADLS 

create external data source gold_layer_source
WITH
(
   location = 'https://storageaccountforamzn.dfs.core.windows.net/gold',
   credential = subhanu_prime
)


create external data source gold_layer_source2
WITH
(
   location = 'https://storageaccountforamzn.blob.core.windows.net/gold',
   credential = subhanu_prime
)

-- create external file format 
create external file format parquet_format
WITH
(
FORMAT_TYPE = PARQUET,
DATA_COMPRESSION = 'org.apache.hadoop.io.compress.SnappyCodec'
)

-- create external tables
create external table goldschema.ext_primefile
with 
(
location= 'ext_prime',
DATA_SOURCE = gold_layer_source,
FILE_FORMAT = parquet_format
) as select * from goldschema.gold_data

select * from goldschema.ext_primefile;
