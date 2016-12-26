package com.hll.dist.common;

import java.io.*;
import java.util.HashMap;

/**
 * 上下文
 * Created by huangll on 2016/12/25.
 */
public class Context {

  private HashMap<String, String> configuration;
  private HashMap<String, Integer> KVBuffer = new HashMap<>();

  @SuppressWarnings("unchecked")
  public Context() throws IOException, ClassNotFoundException {
    File confFile = new File(Constants.TASK_WORK_DIR + File.separator + Constants.CONF_FILE);
    if (confFile.exists()) {
      ObjectInputStream ois = new ObjectInputStream(new FileInputStream(confFile));
      this.configuration = (HashMap<String, String>) ois.readObject();
    } else {
      throw new RuntimeException("job conf not existed...");
    }
  }

  public void write(String k, Integer v) {
    KVBuffer.put(k, v);
  }

  public HashMap<String, Integer> getKVBuffer() {
    return KVBuffer;
  }

  public void setKVBuffer(HashMap<String, Integer> KVBuffer) {
    this.KVBuffer = KVBuffer;
  }

  public HashMap<String, String> getConfiguration() {
    return configuration;
  }

  public void setConfiguration(HashMap<String, String> configuration) {
    this.configuration = configuration;
  }
}
