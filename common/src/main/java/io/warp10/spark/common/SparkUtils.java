package io.warp10.spark.common;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.google.common.base.Charsets;
import io.warp10.script.WarpScriptException;
import scala.Product;

public class SparkUtils {
  public static Object fromSpark(Object o) {
    if (null == o) {
      return null;
    } else if (o instanceof String) {
      return o;
    } else if (o instanceof byte[]) {
      return o;
    } else if (o instanceof BigInteger || o instanceof Long || o instanceof Integer || o instanceof Byte) {
      return ((Number) o).longValue();
    } else if (o instanceof BigDecimal || o instanceof Double || o instanceof Float) {
      return ((Number) o).doubleValue();
    } else if (o instanceof Product) {
      Product prod = (Product) o;
      
      List<Object> list = new ArrayList<Object>();
      scala.collection.Iterator<Object> iter = prod.productIterator();
      
      while(iter.hasNext()) {
        list.add(fromSpark(iter.next()));
      }
      
      return list;
    } else if (o instanceof List) {
      List<Object> l = new ArrayList<Object>();
      for (Object elt : (List) o) {
        l.add(fromSpark(elt));
      }
      return l;
    } else if (o instanceof Iterator) {
      List<Object> l = new ArrayList<Object>();
      while (((Iterator) o).hasNext()) {
        l.add(fromSpark(((Iterator) o).next()));
      }
      return l;
    } else {
      throw new RuntimeException("Encountered yet unsupported type: " + o.getClass());
    }
  }

  public static Object toSpark(Object o) {
    if (null == o) {
      return null;
    } else if (o instanceof String) {
      return o;
    } else if (o instanceof byte[]) {
      return o;
    } else if (o instanceof Number) {
      return o;
    } else if (o instanceof List) {
      ArrayList<Object> l = new ArrayList<Object>();
      
      for (Object elt: (List) o) {
        l.add(toSpark(elt));
      }
      
      return l;
    } else {
      throw new RuntimeException("Encountered yet unsupported type: " + o.getClass());
    }
  }

  /**
   * Parse Warpscript file and return its content as String
   * @param warpscriptName name of the script to parse
   * @return String
   */
  public static String parseScript(String warpscriptName)
      throws IOException, WarpScriptException {

    //
    // Load the Warpscript file
    //
    StringBuffer scriptSB = new StringBuffer();
    InputStream fis = null;
    BufferedReader br = null;
    try {

      fis = SparkUtils.class.getClassLoader().getResourceAsStream(warpscriptName);
      br = new BufferedReader(new InputStreamReader(fis, Charsets.UTF_8));

      while (true) {
        String line = br.readLine();
        if (null == line) {
          break;
        }
        scriptSB.append(line).append("\n");
      }
    } catch (IOException ioe) {
      throw new IOException("Warpscript file should not exist", ioe);
    } finally {
      if (null == br) { try { br.close(); } catch (Exception e) {} }
      if (null == fis) { try { fis.close(); } catch (Exception e) {} }
    }

    return scriptSB.toString();

  }
}
