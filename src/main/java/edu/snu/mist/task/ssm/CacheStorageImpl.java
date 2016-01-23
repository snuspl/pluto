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

package edu.snu.mist.task.ssm;

import org.apache.reef.wake.Identifier;

import java.util.HashMap;

/**
 * This class is the implementation of the CacheStorage.
 * It holds the queryStateMap, which is a nested hash map to contain the states of the operators of the queries.
 */
public class CacheStorageImpl implements CacheStorage{

  private final HashMap<Identifier, HashMap<Identifier, OperatorState>> queryStateMap;

  CacheStorageImpl(){
    queryStateMap = new HashMap<>();
  }

  @Override
  public OperatorState read(final Identifier queryId, final Identifier operatorId){
    OperatorState state = null;
    if (queryStateMap.containsKey(queryId)) {
      state = queryStateMap.get(queryId).get(operatorId);
    }
    return state;
    //TODO [MIST-108]: Return something other than null.
  }

  @Override
  public void addOperatorStateMap(final Identifier queryId,
                                final HashMap<Identifier, OperatorState> operatorStateMap){
    queryStateMap.put(queryId, operatorStateMap);
    //TODO [MIST-50]: caching policy needed.
  }

  @Override
  public boolean update(final Identifier queryId, final Identifier operatorId, final OperatorState state){
    if (queryStateMap.containsKey(queryId)){
      HashMap<Identifier, OperatorState> operatorStateMap = queryStateMap.get(queryId);
      operatorStateMap.put(operatorId, state);
      queryStateMap.put(queryId, operatorStateMap);

      return true;
    } else {
      return false;
    }
  }

  @Override
  public boolean delete(final Identifier queryId){
    if (queryStateMap.containsKey(queryId)){
      queryStateMap.remove(queryId);
      return true;
    } else {
      return false;
    }
  }
}