package io.warp10.spark;

import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

public class PRINT extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
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
