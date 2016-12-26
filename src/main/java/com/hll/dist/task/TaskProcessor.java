package com.hll.dist.task;

import com.hll.dist.common.Constants;
import com.hll.dist.common.Context;
import com.hll.dist.io.InputFormat;
import com.hll.dist.io.OutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;

/**
 * Created by huangll on 2016/12/25.
 */
public class TaskProcessor {

  public static void main(String[] args) throws Exception {
    Context context = new Context();
    HashMap<String, String> conf = context.getConfiguration();

    Class<?> inputFormatClass = Class.forName(conf.get(Constants.INPUT_FORMAT));
    InputFormat inputFormat= (InputFormat) inputFormatClass.newInstance();
    inputFormat.init(context);

    Class<?> logicClass = Class.forName(conf.get(Constants.USER_LOGIC));
    ProcessLogic processLogic= (ProcessLogic) logicClass.newInstance();

    while (inputFormat.hasNext()){
      int key = inputFormat.nextKey();
      String value = inputFormat.nextValue();
      processLogic.process(key,value,context);
    }
    processLogic.cleanUp(context);

    Class<?> outputFormatClass = Class.forName(conf.get(Constants.OUTPUT_FORMAT));
    OutputFormat outputFormat= (OutputFormat) outputFormatClass.newInstance();

    outputFormat.write(context);

    System.exit(0);
  }
}
