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
package edu.snu.mist.task;

import edu.snu.mist.task.executor.MistExecutor;
import org.apache.reef.tang.annotations.DefaultImplementation;

import java.util.List;

/**
 * This provides a list of mist executors which run queries.
 */
@DefaultImplementation(DefaultExecutorListProviderImpl.class)
public interface ExecutorListProvider {

  /**
   * Gets a list of mist executors.
   * @return a list of mist executor
   */
  List<MistExecutor> getExecutors();
}
