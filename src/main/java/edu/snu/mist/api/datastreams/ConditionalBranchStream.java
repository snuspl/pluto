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
package edu.snu.mist.api.datastreams;

/**
 * This interface represents a conditional branch stream.
 * The stream is created inside the continuous stream to diverge.
 * <T> data type of the stream.
 */
interface ConditionalBranchStream<T> extends MISTStream<T> {

  /**
   * Branches out.
   * @param index the index of branch
   * @return new diverged continuous stream
   */
  ContinuousStream<T> branch(int index);
}
