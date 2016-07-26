package io.warp10.spark;

import io.warp10.script.WarpScriptException;

import org.apache.spark.api.java.function.FlatMapFunction2;

public class WarpScriptFlatMapFunction2<T1, T2, R> extends WarpScriptAbstractFunction implements FlatMapFunction2<T1, T2, R> {

  public WarpScriptFlatMapFunction2(String code) throws WarpScriptException {
    super(code);
  }

  @Override
  public Iterable<R> call(T1 t1, T2 t2) throws Exception {
    synchronized(this) {
      getStack().push(SparkUtils.fromSpark(t1));
      getStack().push(SparkUtils.fromSpark(t2));
      getStack().exec(getMacro());
      Object top = getStack().pop();
      
      return (Iterable<R>) SparkUtils.toSpark(top);      
    }
  }
}
