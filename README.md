# Warp10-spark #

Warp10-spark is part of the Warp 10 platform. What are advantages of [Apache Spark](https://spark.apache.org/) ?
- Rich ecosystem
- Well-known
- Hadoop compliant (hdfs, yarn)
- Performance
 
Warp10-spark empowers Apache Spark to manipulate GTS (Geo-Time Series) with Warpscript.
With Warp10-spark you can enrich your Spark applications with the use of Warpscript.

# Warpscript #

The main documentation about WarpScript, Geo-Time Series (GTS) and all about the Warp 10 platform can be found [here](http://www.warp10.io/)

# Prerequisites #

## Technicals ##

- Apache Spark 2.x https://spark.apache.org/downloads.html
- A specific branch dedicated to Apache Spark 1.x is still available but a move to Apache 2.x is recommanded
- Hadoop 1.x / 2.x http://hadoop.apache.org/ (Hadoop is required with the use of Yarn)
- Memory (min): 4 GB

## Purpose ##

I have some data that represents Geo-Time Series and I want to use WarpScript. 

- How can I push my data into Warp10-spark ?
- How can I execute WarpScript code ?
- How can I submit my Warp10-spark application to [Hadoop-Yarn](https://hadoop.apache.org/docs/r2.6.5/hadoop-yarn/hadoop-yarn-site/YARN.html)?  


## Howto ##

### How to push data into Warp10-spark ###

The most classical way to push data to a Warp 10 endpoint is the [Warp 10 Compact Input Format](http://www.warp10.io/apis/gts-input-format).
Thus, this is one way to convert your data into Geo-Time Series thanks to WarpScript.
An example is provided in the unit test ['testWarp10Format'](https://github.com/cityzendata/warp10-spark/blob/master/src/test/java/io/warp10/spark/SparkTest.java)
Fortunately, WarpScript can analyse another kind of format like Json or data that have been serialized, provided that you define a custom parser (WarpScript extension)

### How can I execute WarpScript code ? ###

There are 2 kinds of Function to use WarpScript with Spark:
- WarpScriptFunction / WarpScriptFunction2 that implements org.apache.spark.api.java.function.Function
- WarpScriptFlatMapFunction / WarpScriptFlatMapFunction2 that implements org.apache.spark.api.java.function.FlatMapFunction

Examples are provided in ['SparkTest.java'](https://github.com/cityzendata/warp10-spark/blob/master/src/test/java/io/warp10/spark/SparkTest.java)

WarpScript code can be provided as String directly or by passing through a file that contains the WarpScript

The main WarpScript functions are referennced and described [here](http://www.warp10.io/reference/reference/)

### How can I submit my Warp10-spark application to Hadoop-Yarn ? ###

Here after the command to submit your app with Warp10-Spark to your own Yarn cluster:

~~~
spark-submit --conf spark.executor.extraJavaOptions=-Dwarp10.config=warp10-spark.conf --conf spark.driver.extraJavaOptions=-Dwarp10.config=warp10-spark.conf --deploy-mode cluster --supervise --verbose --master yarn --class io.warp10.spark.SparkTest --files /XXX/warp10-spark.conf,/XXX/test.mc2 ~/XXX/build/libs/myapp.jar
~~~