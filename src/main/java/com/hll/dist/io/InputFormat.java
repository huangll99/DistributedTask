package com.hll.dist.io;

import com.hll.dist.common.Context;

/**
 * 待处理数据的输入接口
 * Created by huangll on 2016/12/25.
 */
public interface InputFormat {
  /**
   * 初始化
   * @param context 上下文
   * @throws Exception
   */
  void init(Context context) throws Exception;

  /**
   * 是否有下一条数据
   * @return
   * @throws Exception
   */
  boolean hasNext() throws Exception;

  /**
   * 获取key
   * @return
   */
  int nextKey();

  /**
   * 获取value
   * @return
   */
  String nextValue();
}
