package io.warp10.spark;

import io.warp10.script.WarpScriptException;

import org.apache.spark.api.java.function.FlatMapFunction;

public class WarpScriptFlatMapFunction<T, R> extends WarpScriptAbstractFunction implements FlatMapFunction<T, R> {

  public WarpScriptFlatMapFunction(String code) throws WarpScriptException {
    super(code);
  }

  @Override
  public Iterable<R> call(T t) throws Exception {
    synchronized(this) {
      getStack().push(SparkUtils.fromSpark(t));
      getStack().exec(getMacro());
      Object top = getStack().pop();
        
      return (Iterable<R>) SparkUtils.toSpark(top);      
    }
  }
}
