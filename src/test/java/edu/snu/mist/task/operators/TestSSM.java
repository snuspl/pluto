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
package edu.snu.mist.task.operators;

import edu.snu.mist.task.ssm.OperatorState;
import edu.snu.mist.task.ssm.SSM;
import org.apache.reef.wake.Identifier;

import javax.inject.Inject;
import java.util.HashMap;
import java.util.Map;

/**
 * This is a simple SSM to test stateful operator.
 */
public final class TestSSM implements SSM {
  private final Map<Identifier, Map<Identifier, OperatorState>> queryStates;

  @Inject
  TestSSM() {
    this.queryStates = new HashMap<>();
  }

  @Override
  public boolean create(final Identifier queryId, final Map<Identifier, OperatorState> queryState) {
    queryStates.put(queryId, queryState);
    return true;
  }

  @Override
  public OperatorState read(final Identifier queryId, final Identifier operatorId) {
    return queryStates.get(queryId).get(operatorId);
  }

  @Override
  public boolean update(final Identifier queryId, final Identifier operatorId, final OperatorState state) {
    queryStates.get(queryId).put(operatorId, state);
    return true;
  }

  @Override
  public boolean delete(final Identifier queryId) {
    queryStates.remove(queryId);
    return true;
  }
}