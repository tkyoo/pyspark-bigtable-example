import json

from pyspark.sql import SparkSession

dependencies = [
    "com.google.cloud.bigtable:bigtable-hbase-2.x-hadoop:1.14.0",
    "org.apache.hbase.connectors.spark:hbase-spark:1.0.0",
    "org.apache.hbase:hbase-server:2.2.4",
]

# In this scenario, current path must be added to extraClassPath for finding hbase-site.xml in HBaseContext object.
# credentials.json: GCP service account json key that have proper authorization.
# this setting (using current path credentials.json) is set on hbase-site.xml. 
# you can find other options about GCP, Bigtable in https://github.com/googleapis/java-bigtable-hbase/blob/master/bigtable-client-core-parent/bigtable-hbase/src/main/java/com/google/cloud/bigtable/hbase/BigtableOptionsFactory.java
# BigtableOptionFactory.java

spark = SparkSession.builder \
    .config("spark.driver.extraClassPath", "./") \
    .config("spark.executor.extraClassPath", "./") \
    .config("spark.jars.packages", ",".join(dependencies)) \
    .config("spark.files", "hbase-site.xml,credentials.json") \
    .getOrCreate()

# Catalog dictionary reference: https://hbase.apache.org/2.2/book.html#_sparksql_dataframes
catalog = {
    "table": {
        "namespace": "default",
        "name": "test_table",
    },
    "rowkey": "name",
    "columns": {
        "name": { 
            "cf": "rowkey",
            "col": "name",
            "type": "string"
        },
        "age": {
            "cf": "age",
            "col": "age",
            "type": "bigint"
        }
    }
}

# example data & catalog reference: https://github.com/GoogleCloudPlatform/cloud-bigtable-examples/tree/master/scala/bigtable-shc
people = [
    { "name": "bill", "age": 30 },
    { "name": "solomon", "age": 27 },
    { "name": "misha", "age": 30 },
    { "name": "jeff", "age": 30 },
    { "name": "les", "age": 27 },
    { "name": "ramesh", "age": 31 },
    { "name": "jacek", "age": 45 },
]

# You can find options like 'catalog', 'newtable' in https://github.com/apache/hbase-connectors/blob/master/spark/hbase-spark/src/main/scala/org/apache/hadoop/hbase/spark/datasources/HBaseTableCatalog.scala
# HBaseTableCatalog object.
# options like 'hbase.spark.use.hbasecontext', 'hbase.spark.query.timestamp' are in https://github.com/apache/hbase-connectors/blob/master/spark/hbase-spark/src/main/scala/org/apache/hadoop/hbase/spark/datasources/HBaseSparkConf.scala
# HBaseSparkConf object.

# The difference between 'hortonworks-spark/shc' and 'apache/hbase-connector' is a 'format'. 
# If you use 'hortonworks-spark/shc', you must use 'org.apache.spark.sql.execution.datasources.hbase'.
# In this scenario, I use 'apache/hbase-connector', thus this project must use 'org.apache.hadoop.hbase.spark' format. 

# In my case, I must set 'hbase.spark.use.hbasecontext' to false. if not, hbase-connector cannot find 'hbase-site.xml' and you can see zookeepr connection errors.
spark.createDataFrame(people).write \
    .option("catalog", json.dumps(catalog)) \
    .option("newtable", 5) \
    .option("hbase.spark.use.hbasecontext", "false") \
    .format("org.apache.hadoop.hbase.spark") \
    .save()
