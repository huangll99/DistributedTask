package com.hll.dist.io;

import com.hll.dist.common.Constants;
import com.hll.dist.common.Context;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

/**
 * Created by huangll on 2016/12/25.
 */
public class DefaultInputFormat implements InputFormat {

  private String inputPath;
  private BufferedReader br;
  private int key;
  private String value;
  private int lineNumber = 0;

  @Override
  public void init(Context context) throws Exception {
    this.inputPath = context.getConfiguration().get(Constants.INPUT_PATH);
    this.br = new BufferedReader(new FileReader(inputPath));
  }

  @Override
  public boolean hasNext() throws Exception {
    String line = readLine();
    this.key = ++lineNumber;
    this.value = line;
    return null != line;
  }

  private String readLine() throws IOException {
    String line = br.readLine();
    if (line == null) {
      br.close();
    }
    return line;
  }

  @Override
  public int nextKey() {
    return this.key;
  }

  @Override
  public String nextValue() {
    return this.value;
  }
}
