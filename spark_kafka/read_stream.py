import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types

# add more functions as necessary

def main(stream_name):
    messages = spark.readStream.format('kafka') \
        .option('kafka.bootstrap.servers', 'localhost:9092,localhost:9092,localhost:9094') \
        .option('subscribe', stream_name).load()

    values_string = messages.select(messages['value'].cast('string'))
    
    #lets split the values
    col_split = functions.split(values_string.value, " ")
    x = col_split.getItem(0)
    y = col_split.getItem(1)
    
    values_string = values_string.withColumn("x", x)
    values_string = values_string.withColumn("y", y)
    values_string.createOrReplaceTempView("split_table")
    
    new_df = spark.sql("""
                       SELECT x,y, POWER(x, 2) as x_square, POWER(y, 2) as y_square, x*y as xy_mult  FROM 
                       split_table
                       """)
    new_df.createOrReplaceTempView("power_table")
    
    sum_df = spark.sql("""
                       SELECT SUM(x) as x_sum, SUM(y) as y_sum, SUM(x_square) as xsquare_sum, SUM(y_square) as ysquare_sum, SUM(xy_mult) as xy_sum, COUNT(x) as count
                       FROM power_table
                       """)
    sum_df.createOrReplaceTempView("sum_table")
    
    semi_final_df = spark.sql("""
                         SELECT
                         * , (((xy_sum) - ((1 / count) * x_sum * y_sum)) / (xsquare_sum - ((1 / count) * POWER(x_sum, 2)))) as slope
                         FROM sum_table
                         """)
    semi_final_df.createOrReplaceTempView("semi_final_table")
    
    final_df = spark.sql("""
                         SELECT slope, 
                         ( (y_sum - (slope * x_sum) ) / (count) ) as intercept
                         FROM semi_final_table
                         """)
    
    stream =final_df.writeStream.outputMode("update").format("console").start()
    stream.awaitTermination(600)
    
if __name__ == '__main__':
    stream_name = sys.argv[1]
    spark = SparkSession.builder.appName('example code').getOrCreate()
    assert spark.version >= '2.4' # make sure we have Spark 2.4+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(stream_name)
