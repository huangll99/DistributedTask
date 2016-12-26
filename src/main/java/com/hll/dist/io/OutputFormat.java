package com.hll.dist.io;

import com.hll.dist.common.Context;

/**
 * 计算结果输出接口
 * Created by huangll on 2016/12/25.
 */
public interface OutputFormat {

  void write(Context context) throws Exception;

  void cleanUp() throws Exception;
}
