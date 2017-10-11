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
package edu.snu.mist.core.task;

import edu.snu.mist.core.task.globalsched.Group;
import org.apache.reef.tang.annotations.DefaultImplementation;

import java.util.concurrent.atomic.AtomicLong;

@DefaultImplementation(DefaultQueryImpl.class)
public interface Query {

  void setGroup(Group group);

  void insert(SourceOutputEmitter sourceOutputEmitter);

  void delete(SourceOutputEmitter sourceOutputEmitter);

  int processAllEvent();

  int size();

  //int numEvents();

  String getId();

  Group getGroup();

  AtomicLong getProcessingTime();

  AtomicLong getProcessingEvent();

  void setLoad(double load);

  double getLoad();

  long getLatestRebalanceTime();

  void setLatestRebalanceTime(long t);

  long numberOfRemainingEvents();
}