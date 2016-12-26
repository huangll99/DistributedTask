package com.hll.dist.io;

import com.hll.dist.common.Constants;
import com.hll.dist.common.Context;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Created by huangll on 2016/12/25.
 */
public class DefaultOutputFormat implements OutputFormat {

  private BufferedWriter bw;

  @Override
  public void write(Context context) throws Exception {
    String outputPath = context.getConfiguration().get(Constants.OUTPUT_PATH);
    HashMap<String, Integer> kvBuffer = context.getKVBuffer();
    bw = new BufferedWriter(new FileWriter(outputPath));
    Set<Map.Entry<String, Integer>> entrySet = kvBuffer.entrySet();
    for (Map.Entry entry : entrySet) {
      bw.write(entry.getKey() + "\t" + entry.getValue() + "\n\r");
    }
    bw.flush();
  }

  @Override
  public void cleanUp() throws Exception {
    bw.close();
  }
}
