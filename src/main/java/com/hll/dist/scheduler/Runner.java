package com.hll.dist.scheduler;

import com.hll.dist.common.Constants;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.HashMap;

/**
 * Created by huangll on 2016/12/25.
 */
public class Runner {

  private HashMap<String,String> conf;

  public Runner(HashMap<String, String> conf) {
    this.conf = conf;
  }

  public void submit() throws IOException {
    ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(Constants.CONF_FILE));
    oos.writeObject(conf);

    WorkerClient workerClient = new WorkerClient(conf);
    workerClient.submit();
  }
}
