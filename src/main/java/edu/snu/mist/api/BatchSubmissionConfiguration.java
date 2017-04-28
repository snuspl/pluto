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
package edu.snu.mist.api;

import edu.snu.mist.common.functions.MISTFunction;
import org.apache.reef.tang.formats.*;

import java.util.List;

/**
 * Configuration class for setting batch query submission.
 * Submitted query will be duplicated according to this configuration.
 */
public final class BatchSubmissionConfiguration extends ConfigurationModuleBuilder {

  /**
   * A function generates publish topic from a group name.
   */
  private final MISTFunction<String, String> pubTopicGenerateFunc;

  /**
   * A function generates subscribe topic name from a group name.
   */
  private final MISTFunction<String, String> subTopicGenerateFunc;

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

  public BatchSubmissionConfiguration(final MISTFunction<String, String> pubTopicGenerateFunc,
                                      final MISTFunction<String, String> subTopicGenerateFunc,
                                      final List<Integer> queryGroupList,
                                      final int startQueryNum,
                                      final int batchSize) {
    this.pubTopicGenerateFunc = pubTopicGenerateFunc;
    this.subTopicGenerateFunc = subTopicGenerateFunc;
    this.queryGroupList = queryGroupList;
    this.startQueryNum = startQueryNum;
    this.batchSize = batchSize;
  }

  public MISTFunction<String, String> getPubTopicGenerateFunc() {
    return pubTopicGenerateFunc;
  }

  public MISTFunction<String, String> getSubTopicGenerateFunc() {
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