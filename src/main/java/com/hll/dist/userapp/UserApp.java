package com.hll.dist.userapp;

import com.hll.dist.common.Constants;
import com.hll.dist.scheduler.Runner;

import java.io.IOException;
import java.util.HashMap;

/**
 * Created by huangll on 2016/12/25.
 */
public class UserApp {

  public static void main(String[] args) throws IOException {

    HashMap<String,String> conf = new HashMap<>();
    conf.put(Constants.INPUT_PATH,"c:/a.txt");
    conf.put(Constants.OUTPUT_PATH,"c:/out.txt");
    conf.put(Constants.INPUT_FORMAT,"com.hll.dist.io.DefaultInputFormat");
    conf.put(Constants.OUTPUT_FORMAT,"com.hll.dist.io.DefaultOutputFormat");
    conf.put(Constants.JAR_PATH,"c:/DistributedTask-1.0.jar");
    conf.put(Constants.WORKER_HOST,"localhost");
    conf.put(Constants.WORKER_PORT,"9999");
    conf.put(Constants.USER_LOGIC,"com.hll.dist.userapp.WordCount");

    Runner runner = new Runner(conf);
    runner.submit();
  }
}
