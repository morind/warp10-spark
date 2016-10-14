package io.warp10.spark2;

import java.util.ArrayList;
import java.util.List;

import io.warp10.WarpConfig;
import io.warp10.hadoop.Warp10InputFormat;
import io.warp10.spark.common.SparkUtils;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Test;

public class SparkTest {

  public static void main(String... args) {
    try {
      SparkTest sparkTest = new SparkTest();
      SparkConf conf = new SparkConf().setAppName("testapp").setMaster(args[0]);
      conf.set("token", args[1]);
      sparkTest.testWarp10InputFormat(conf);
    } catch (Exception e) {
      System.err.println(e.getMessage());
    }
  }

  public void test() throws Exception {
    System.setProperty("warp.timeunits", "us");
    WarpConfig.setProperties((String) null);
    test(new SparkConf().setAppName("test").setMaster("local"));
  }

  @Test
  public void testWarp10InputFormat() throws Exception {
    System.setProperty("warp.timeunits", "us");
    WarpConfig.setProperties((String) null);
    testWarp10InputFormat(new SparkConf().setAppName("testWarp10InputFormat").setMaster("local"));
  }

  public void test(SparkConf conf) throws Exception {

    JavaSparkContext sc = new JavaSparkContext(conf);

    List<String> list = new ArrayList<String>();

    for (int i = 0; i < 1280; i++) {

      list.add(Long.toString(i));

    }

    JavaRDD<String> lines = sc.parallelize(list, 10);

    lines = lines.keyBy(new WarpScriptFunction<>("0 1 SUBSTRING")).flatMap(new WarpScriptFlatMapFunction<>("SNAPSHOT [ SWAP ]"));

    System.out.println(lines.collect());

  }


  public void testWarp10InputFormat(SparkConf conf) throws Exception {

    JavaSparkContext sc = new JavaSparkContext(conf);

    sc.hadoopConfiguration().set("warp10.fetcher.protocol","http");
    sc.hadoopConfiguration().set("warp10.fetcher.fallbacks","localhost");
    sc.hadoopConfiguration().set("http.header.now","X-CityzenData-Now");
    sc.hadoopConfiguration().set("http.header.timespan","X-Warp10-Timespan");
    sc.hadoopConfiguration().set("warp10.fetcher.port","8881");
    sc.hadoopConfiguration().set("warp10.fetcher.path","/api/v0/sfetch");
    sc.hadoopConfiguration().set("warp10.splits.endpoint","https://warp.cityzendata.net/api/v0/splits");
    sc.hadoopConfiguration().set("warp10.fetch.timespan","-10");
    sc.hadoopConfiguration().set("warp10.http.connect.timeout","10000");
    sc.hadoopConfiguration().set("warp10.http.read.timeout","10000");
    sc.hadoopConfiguration().set("warp10.max.splits","10");

    sc.hadoopConfiguration().set("warp10.fetcher.fallbacksonly", "true");

    sc.hadoopConfiguration().set("warp10.splits.token", conf.get("token"));
    sc.hadoopConfiguration().set("warp10.splits.selector", "~.*{}");
    sc.hadoopConfiguration().set("warp10.fetch.now", "1444000000000000");
    sc.hadoopConfiguration().set("warp10.fetch.timespan", "600000000000000");

    System.out.println("testWarp10InputFormat");

    JavaPairRDD<Text, BytesWritable> inputRDD = sc.newAPIHadoopFile("test", Warp10InputFormat.class, Text.class, BytesWritable.class, sc.hadoopConfiguration());

    //lines = lines.keyBy(new WarpScriptFunction<>("0 1 SUBSTRING")).flatMap(new WarpScriptFlatMapFunction<>("SNAPSHOT [ SWAP ]"));

    JavaRDD<String> lines = inputRDD.values().map(value -> SparkUtils.GTSDump(value.getBytes(), true));

    System.out.println(lines.collect());

  }

}