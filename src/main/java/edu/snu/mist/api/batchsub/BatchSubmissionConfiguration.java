/*
 * Copyright (C) 2017 Seoul National University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.snu.mist.api.batchsub;

import edu.snu.mist.common.functions.MISTBiFunction;

import java.util.List;
import java.util.Set;

/**
 * TODO[DELETE] this code is for test.
 * Configuration class for setting batch query submission.
 * Submitted query will be duplicated according to this configuration.
 */
public final class BatchSubmissionConfiguration {

  /**
   * A function generates MQTT sink topic to publish from a group id and query number.
   * The first parameter should be group Id.
   */
  private final MISTBiFunction<String, Integer, String> pubTopicGenerateFunc;

  /**
   * A function generates a set of MQTT sink topic to subscribe from a group id and query number.
   * The first parameter should be group Id.
   */
  private final MISTBiFunction<String, Integer, Set<String>> subTopicGenerateFunc;

  /**
   * A list represents the number of queries per each groups.
   */
  private final List<Integer> queryGroupList;

  /**
   * A query number represents the starting point of query group list.
   */
  private final int startQueryNum;

  /**
   * A batch size represents how many queries will be generated.
   */
  private final int batchSize;

  public BatchSubmissionConfiguration(final MISTBiFunction<String, Integer, Set<String>> subTopicGenerateFunc,
                                      final MISTBiFunction<String, Integer, String> pubTopicGenerateFunc,
                                      final List<Integer> queryGroupList,
                                      final int startQueryNum,
                                      final int batchSize) {
    this.subTopicGenerateFunc = subTopicGenerateFunc;
    this.pubTopicGenerateFunc = pubTopicGenerateFunc;
    this.queryGroupList = queryGroupList;
    this.startQueryNum = startQueryNum;
    this.batchSize = batchSize;
  }

  public MISTBiFunction<String, Integer, String> getPubTopicGenerateFunc() {
    return pubTopicGenerateFunc;
  }

  public MISTBiFunction<String, Integer, Set<String>> getSubTopicGenerateFunc() {
    return subTopicGenerateFunc;
  }

  public List<Integer> getQueryGroupList() {
    return queryGroupList;
  }

  public int getStartQueryNum() {
    return startQueryNum;
  }

  public int getBatchSize() {
    return batchSize;
  }
}