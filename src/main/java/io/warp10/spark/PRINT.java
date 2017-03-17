package io.warp10.spark;

import java.util.List;
import java.util.Map;

import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;

import scala.Function1;
import scala.runtime.BoxedUnit;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptLib;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

public class PRINT extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  private io.warp10.script.functions.FOREACH original = new io.warp10.script.functions.FOREACH("FOREACH");
  
  public PRINT(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {

    Object top = stack.pop();

    System.out.println(top);
    
    return stack;
  }
}
