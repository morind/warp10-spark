package io.warp10.spark;

import io.warp10.script.MemoryWarpScriptStack;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStack.Macro;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.Serializable;
import java.util.Properties;

public abstract class WarpScriptAbstractFunction implements Serializable {
  
  private WarpScriptStack stack;

  private String mc2 = "<% %>";
  
  private Macro macro = null;
  
  public WarpScriptAbstractFunction() {
    this.stack = null;
  }

  public WarpScriptAbstractFunction(String code) throws WarpScriptException {
    this.stack = null;
    setCode(code);
  }

  public void setCode(String code) throws WarpScriptException {
    initStack();
    init(code);
  }

  private void writeObject(java.io.ObjectOutputStream out) throws IOException {    
    out.writeUTF(this.mc2);
  }
  
  private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
    String code = in.readUTF();
    try {
      initStack();
      init(code);    
    } catch (WarpScriptException wse) {
      throw new IOException(wse);
    } catch (Throwable t) {
      t.printStackTrace();
    }
  }

  private void initStack() {
    Properties properties = new Properties();
    
    MemoryWarpScriptStack stack = new MemoryWarpScriptStack(null, null, properties);
    stack.maxLimits();
    
    this.stack = stack;
  }

  private void init(String code) throws WarpScriptException {
    
    if (code.startsWith("@")) {
      Reader reader = null;
      BufferedReader br = null;
      StringBuilder sb = new StringBuilder();
      
      try {
        reader = new InputStreamReader(WarpScriptAbstractFunction.class.getClassLoader().getResourceAsStream(code.substring(1)));
        //reader = new FileReader(code.substring(1));
        br = new BufferedReader(reader);        
        
        while(true) {
          String line = br.readLine();
          
          if (null == line) {
            break;
          }
        
          sb.append(line);
          sb.append("\n");
        }
      } catch (IOException ioe) {
        throw new WarpScriptException(ioe);
      } finally {
        if (null == br) { try { br.close(); } catch (Exception e) {} }
        if (null == reader) { try { reader.close(); } catch (Exception e) {} }
      }
      
      code = sb.toString();
    }
    
    this.mc2 = code;

    stack.execMulti(code);
    
    Object top = stack.pop();
    
    if (!(top instanceof Macro)) {
      throw new WarpScriptException("WarpScipt code should leave a macro on top of the stack.");
    }
    
    this.macro = (Macro) top;
  }
  
  public WarpScriptStack getStack() {
    return this.stack;
  }
  
  public WarpScriptStack.Macro getMacro() {
    return this.macro;
  }
}
