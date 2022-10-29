from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType


spark = SparkSession.builder.appName("AMOUNT CUSTOMER").getOrCreate()

schema = StructType([ \
    StructField("cust_ID", IntegerType(), True), \
    StructField("item_id", IntegerType(), True), \
    StructField("amount_spent", FloatType(), True)])

customerDF = df = spark.read.schema(schema).csv("file:///SparkCourse/customer-orders.csv")
df.printSchema()


totalBYCustomer = customerDF.groupBy('cust_ID').agg(func.round(func.sum("amount_spent"),2)\
    .alias("total_spent"))

totalByCustomerSorted = totalBYCustomer.sort("total_spent")
totalByCustomerSorted.show(totalByCustomerSorted.count())

spark.stop()
