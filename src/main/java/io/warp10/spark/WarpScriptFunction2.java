package io.warp10.spark;

import io.warp10.script.WarpScriptException;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

public class WarpScriptFunction2<T1, T2, R> extends WarpScriptAbstractFunction implements Function2<T1, T2, R> {

  public WarpScriptFunction2(String code) throws WarpScriptException {
    super(code);
  }

  @Override
  public R call(T1 v1, T2 v2) throws Exception {
    synchronized(this) {
      getStack().push(SparkUtils.fromSpark(v1));
      getStack().push(SparkUtils.fromSpark(v2));
      getStack().exec(getMacro());
      Object top = getStack().pop();
      
      return (R) SparkUtils.toSpark(top);      
    }
  }
}
