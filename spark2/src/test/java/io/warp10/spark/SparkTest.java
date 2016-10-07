package io.warp10.spark;

import java.util.ArrayList;
import java.util.List;

import io.warp10.WarpConfig;
import io.warp10.spark2.WarpScriptFlatMapFunction;
import io.warp10.spark2.WarpScriptFunction;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Test;
import scala.Tuple2;

public class SparkTest {

  public static void main(String... args) {
    try {
      SparkTest sparkTest = new SparkTest();
      sparkTest.test(new SparkConf().setAppName("testapp"));
    } catch (Exception e) {
      System.err.println(e.getMessage());
    }
  }

  @Test
  public void test() throws Exception {
    test(new SparkConf().setAppName("testapp").setMaster("local"));
  }

  public void test(SparkConf conf) throws Exception {

    System.out.println(this.getClass().getClassLoader());

    System.setProperty("warp.timeunits", "us");

    WarpConfig.setProperties((String) null);

    JavaSparkContext sc = new JavaSparkContext(conf);

    List<String> list = new ArrayList<String>();

    for (int i = 0; i < 1280; i++) {

      list.add(Long.toString(i));

    }

    JavaRDD<String> lines = sc.parallelize(list, 10);

    lines = lines.keyBy(new WarpScriptFunction<>("0 1 SUBSTRING")).flatMap(new WarpScriptFlatMapFunction<>("SNAPSHOT [ SWAP ]"));

    System.out.println(lines.collect());

  }

}