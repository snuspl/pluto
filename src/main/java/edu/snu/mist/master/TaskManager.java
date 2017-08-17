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
package edu.snu.mist.master;

import org.apache.reef.tang.annotations.DefaultImplementation;

/**
 * The interface which manages evaluators in mist master.
 */
@DefaultImplementation(DefaultTaskManagerImpl.class)
public interface TaskManager extends AutoCloseable {
  // TODO[MIST-423]: Implement tracking evaluator stats in mist master
}
