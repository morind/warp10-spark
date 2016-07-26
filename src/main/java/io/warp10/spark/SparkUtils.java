package io.warp10.spark;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import scala.Product;
import scala.Tuple1;

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
      for (Object elt: (List) o) {
        l.add(fromSpark(elt));
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

}
