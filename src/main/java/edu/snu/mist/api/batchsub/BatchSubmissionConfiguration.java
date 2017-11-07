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
   * A function generates MQTT sink topic to publish from a group id and query id.
   * The first parameter should be group Id.
   */
  private final MISTBiFunction<String, String, String> pubTopicGenerateFunc;

  /**
   * A function generates a set of MQTT sink topic to subscribe from a group id and query id.
   * The first parameter should be group Id.
   */
  private final MISTBiFunction<String, String, Set<String>> subTopicGenerateFunc;

  /**
   * A list of super group id.
   */
  private final List<String> superGroupIdList;

  /**
   * A list of sub group id.
   */
  private final List<String> subGroupIdList;

  /**
   * The factor value which indicates how many queries can be merged into one.
   */
  private int mergeFactor;

  public BatchSubmissionConfiguration(final MISTBiFunction<String, String, Set<String>> subTopicGenerateFunc,
                                      final MISTBiFunction<String, String, String> pubTopicGenerateFunc,
                                      final List<String> superGroupIdList,
                                      final List<String> subGroupIdList,
                                      final int mergeFactor) {
    this.subTopicGenerateFunc = subTopicGenerateFunc;
    this.pubTopicGenerateFunc = pubTopicGenerateFunc;
    this.superGroupIdList = superGroupIdList;
    this.subGroupIdList = subGroupIdList;
    this.mergeFactor = mergeFactor;
  }

  public BatchSubmissionConfiguration(final MISTBiFunction<String, String, Set<String>> subTopicGenerateFunc,
                                      final MISTBiFunction<String, String, String> pubTopicGenerateFunc,
                                      final List<String> superGroupIdList,
                                      final List<String> subGroupIdList) {
    this.subTopicGenerateFunc = subTopicGenerateFunc;
    this.pubTopicGenerateFunc = pubTopicGenerateFunc;
    this.superGroupIdList = superGroupIdList;
    this.subGroupIdList = subGroupIdList;
  }

  public MISTBiFunction<String, String, String> getPubTopicGenerateFunc() {
    return pubTopicGenerateFunc;
  }

  public MISTBiFunction<String, String, Set<String>> getSubTopicGenerateFunc() {
    return subTopicGenerateFunc;
  }

  public List<String> getSuperGroupIdList() {
    return superGroupIdList;
  }

  public List<String> getSubGroupIdList() {
    return subGroupIdList;
  }

  // TODO:
  public int getMergeFactor() {
    return mergeFactor;
  }
}