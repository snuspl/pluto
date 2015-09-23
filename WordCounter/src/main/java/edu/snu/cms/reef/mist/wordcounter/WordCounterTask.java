/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package edu.snu.cms.reef.mist.wordcounter;

import org.apache.reef.task.Task;
import org.codehaus.jackson.map.ser.impl.SerializerCache;

import javax.inject.Inject;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * A 'hello REEF' Task.
 */
public final class WordCounterTask implements Task {

  Random _rand;
  Map<String, Integer> counts = new HashMap<String, Integer>();

  private String generator() {
    String[] sentences = new String[]{ "the cow jumped over the moon", "an apple a day keeps the doctor away",
            "four score and seven years ago", "snow white and the seven dwarfs", "i am at two with nature" };
    String sentence = sentences[_rand.nextInt(sentences.length)];
    return sentence;
  }

  private String[] splitter(String sentence) {
    String[] words = sentence.split(" ");
    return words;
  }

  private void counter (String word) {
      Integer count = counts.get(word);
      if (count == null)
        count = 0;
      count++;
      counts.put(word, count);
  }

  @Inject
  private WordCounterTask() {
    _rand = new Random();
  }

  @Override
  public byte[] call(final byte[] memento) {
    for(int i = 0; i < 10; i++) {
      String[] words = splitter(generator());
      for(String word : words) {
        counter(word);
      }
    }
    for(Map.Entry<String, Integer> item : counts.entrySet()) {
      System.out.println("Word: " + item.getKey() + ", Count: " + item.getValue());
    }
    return null;
  }
}

