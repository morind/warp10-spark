package io.warp10.pyspark;

import io.warp10.spark.SparkTest;
import org.apache.spark.SparkConf;
import org.junit.Test;

import java.util.Map;

public class PySparkCallerTest {

  @Test
  public void testWarp10Format(Map<String,String> kv) throws Exception {
    PySparkCaller pySparkCaller = new PySparkCaller();
    SparkConf sparkConf = pySparkCaller.getConf(kv);
    SparkTest sparkTest = new SparkTest();
    sparkTest.testWarp10Format(sparkConf);
  }

  @Test
  public void testWarpScriptFile(Map<String,String> kv) throws Exception {
    PySparkCaller pySparkCaller = new PySparkCaller();
    SparkConf sparkConf = pySparkCaller.getConf(kv);
    SparkTest sparkTest = new SparkTest();
    sparkTest.testWarpScriptFile(sparkConf);
  }

}
