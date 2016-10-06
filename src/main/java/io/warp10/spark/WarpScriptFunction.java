package io.warp10.spark;

import io.warp10.script.WarpScriptException;

import org.apache.spark.api.java.function.Function;

public class WarpScriptFunction<T, R> extends WarpScriptAbstractFunction implements Function<T, R> {

  public WarpScriptFunction(String code) throws WarpScriptException {
    super(code);
  }

  @Override
  public R call(T v1) throws Exception {
    synchronized(this) {
      getStack().push(SparkUtils.fromSpark(v1));
      getStack().exec(getMacro());
      Object top = getStack().pop();
      
      return (R) SparkUtils.toSpark(top);      
    }
  }
}
