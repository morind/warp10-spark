package io.warp10.pyspark;

import io.warp10.continuum.Configuration;
import io.warp10.script.WarpScriptStack;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.IOException;
import java.util.Map;

public class PySparkCaller {

  private static final Logger LOG = LoggerFactory.getLogger(PySparkCaller.class);

  public SparkConf getConf(Map<String,String> kv) throws IOException {
    try {
      LOG.info("PySparkCaller");

      SparkConf conf = new SparkConf();
      SparkContext.getOrCreate(conf).stop();

      LOG.info("Dump params");
      kv.forEach((k, v) -> {
        LOG.info(k + " -> " + v);
        if ("master".equals(k)) {
          conf.setMaster(v);
        } else if ("appName".equals(k)) {
          conf.setAppName(v);
        } else if ("warp.timeunits".equals(k)) {
          System.setProperty(Configuration.WARP_TIME_UNITS, v);
        } else {
          conf.set(k, v);
        }
      });

      JavaSparkContext sc = JavaSparkContext.fromSparkContext(SparkContext.getOrCreate(conf));

      LOG.info("Jars");
      for (String jar: sc.jars()) {
        LOG.info("jar: " + jar);
      }

      LOG.info("Dump SparkConf");
      for (Tuple2<String, String>  t: sc.getConf().getAll()) {
        LOG.info("key: " + t._1());
        LOG.info("value: " + t._2());
      }

      return conf;

    } catch (Exception e) {
      throw new IOException(e.getMessage(),e);
    }
  }
}
