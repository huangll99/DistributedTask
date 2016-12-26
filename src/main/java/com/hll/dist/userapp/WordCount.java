package com.hll.dist.userapp;

import com.hll.dist.common.Context;
import com.hll.dist.task.ProcessLogic;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by huangll on 2016/12/25.
 */
public class WordCount extends ProcessLogic {

  private Map<String, Integer> wordCount = new HashMap<>();

  @Override
  public void process(Integer k, String v, Context context) {
    String[] words = v.split(" ");
    for (String word : words) {
      Integer count = wordCount.get(word);
      if (count == null) {
        wordCount.put(word, 1);
      } else {
        wordCount.put(word, count + 1);
      }
    }
  }

  @Override
  public void cleanUp(Context context) {
    wordCount.entrySet().stream().forEach(entry -> context.write(entry.getKey(), entry.getValue()));
  }
}
