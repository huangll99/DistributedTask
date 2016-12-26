package com.hll.dist.task;

import com.hll.dist.common.Context;

/**
 * Created by huangll on 2016/12/25.
 */
public abstract class ProcessLogic {
  public abstract void process(Integer k, String v, Context context);

  public void cleanUp(Context context) {
  }
}
