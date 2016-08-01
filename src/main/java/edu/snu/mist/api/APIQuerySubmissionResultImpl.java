/*
 * Copyright (C) 2016 Seoul National University
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

import edu.snu.mist.formats.avro.IPAddress;

/**
 * Basic Implementation of APIQuerySubmissionResult.
 */
public class APIQuerySubmissionResultImpl implements APIQuerySubmissionResult {

  private final String queryId;
  private final IPAddress taskAddress;

  public APIQuerySubmissionResultImpl(final String queryId, final IPAddress taskAddress) {
    this.queryId = queryId;
    this.taskAddress = taskAddress;
  }

  public APIQuerySubmissionResultImpl(final CharSequence queryId, final IPAddress taskAddress) {
    this(queryId.toString(), taskAddress);
  }

  @Override
  public String getQueryId() {
    return this.queryId;
  }

  @Override
  public IPAddress getTaskAddress() {
    return this.taskAddress;
  }
}
