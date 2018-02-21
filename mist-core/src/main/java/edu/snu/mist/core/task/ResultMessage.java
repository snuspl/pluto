/*
 * Copyright (C) 2018 Seoul National University
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

package edu.snu.mist.core.task;

/**
 * It is a utility class for the client messages.
 */
public final class ResultMessage {
  public static String noQueryId(final String queryId) {
    return String.format("Fail: There is no %s.", queryId);
  }

  public static String noLogicalPlan(final String queryId) {
    return String.format("Fail: There is no %s's LogicalPlan.", queryId);
  }

  public static String planDeletionFail(final String queryId) {
    return String.format("Fail: cannot delete %s's LogicalPlan.", queryId);
  }

  public static String injectionFailure() {
    return "Fail: Injection Exception occurred during de-serializing LogicalPlans.";
  }

  public static String resumeSuccess(final String queryId) {
    return String.format("Success: %s is resumed", queryId);
  }

  public static String stopSuccess(final String queryId) {
    return String.format("Success: %s is stopped", queryId);
  }

  public static String deleteSuccess(final String queryId) {
    return String.format("Success: %s is deleted", queryId);
  }

  public static String submitSuccess(final String queryId) {
    return String.format("Success: %s is submitted", queryId);
  }

  private ResultMessage() {

  }
}
