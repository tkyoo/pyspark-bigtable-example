import json

from pyspark.sql import SparkSession

dependencies = [
    "com.google.cloud.bigtable:bigtable-hbase-2.x:1.14.0",
    "org.apache.hbase.connectors.spark:hbase-spark:1.0.0",
    "org.apache.hbase:hbase-server:2.2.3",
]

spark = SparkSession.builder \
    .config("spark.driver.extraClassPath", "./") \
    .config("spark.executor.extraClassPath", "./") \
    .config("spark.jars.packages", ",".join(dependencies)) \
    .config("spark.files", "hbase-site.xml,credentials.json") \
    .getOrCreate()

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

people = [
    { "name": "bill", "age": 30 },
    { "name": "solomon", "age": 27 },
    { "name": "misha", "age": 30 },
    { "name": "jeff", "age": 30 },
    { "name": "les", "age": 27 },
    { "name": "ramesh", "age": 31 },
    { "name": "jacek", "age": 45 },
]

spark.createDataFrame(people).write \
    .option("catalog", json.dumps(catalog)) \
    .option("newtable", 5) \
    .option("hbase.spark.use.hbasecontext", "false") \
    .format("org.apache.hadoop.hbase.spark") \
    .save()
