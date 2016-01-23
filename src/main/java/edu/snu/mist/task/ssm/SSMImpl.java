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

import edu.snu.mist.task.ssm.orientdb.*;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.wake.Identifier;

import javax.inject.Inject;
import java.io.Serializable;
import java.util.HashMap;

/**
 * This class is the implementation of the SSM.
 */
public final class SSMImpl implements SSM, Serializable{

  private static CacheStorage cacheStorage;
  private static DatabaseStorage databaseStorage;
  private Identifier queryId;

  @Inject
  private SSMImpl(final Identifier queryId,
                   @Parameter(OrientDbPath.class) final String orientDbPath,
                   @Parameter(OrientDbUser.class) final String orientDbUser,
                   @Parameter(OrientDbPassword.class) final String orientDbPassword,
                   @Parameter(OrientDbDropDb.class) final boolean orientDbDrop,
                   @Parameter(OrientDbSize.class) final int orientDbSize)
      throws DatabaseOpenException, InjectionException{
    cacheStorage = new CacheStorageImpl();
    final Injector injector = Tang.Factory.getTang().newInjector();
    injector.bindVolatileParameter(OrientDbPath.class, orientDbPath);
    injector.bindVolatileParameter(OrientDbUser.class, orientDbUser);
    injector.bindVolatileParameter(OrientDbPassword.class, orientDbPassword);
    injector.bindVolatileParameter(OrientDbDropDb.class, orientDbDrop);
    injector.bindVolatileParameter(OrientDbSize.class, orientDbSize);
    databaseStorage = injector.getInstance(DatabaseStorageOrientDbImpl.class);
    this.queryId = queryId;
  }

  @Override
  public void create(final HashMap<Identifier, OperatorState> operatorStateMap) {
    cacheStorage.addOperatorStateMap(queryId, operatorStateMap);
  }

  public OperatorState read(final Identifier operatorId) throws DatabaseReadException {
    final OperatorState state = cacheStorage.read(queryId, operatorId);
    if (state == null){
      HashMap<Identifier, OperatorState> operatorStateMap = databaseStorage.read(queryId);
      cacheStorage.addOperatorStateMap(queryId, operatorStateMap);
    }

    return state;
  }

  public boolean update(final Identifier operatorId, final OperatorState state) throws DatabaseReadException{
    if(cacheStorage.update(queryId, operatorId, state)){
      return true;
    } else {
      HashMap<Identifier, OperatorState> operatorStateMap = databaseStorage.read(queryId); //read from the database
      cacheStorage.addOperatorStateMap(queryId, operatorStateMap); //store the map into the queryStateMap in the memory.
      return cacheStorage.update(queryId, operatorId, state); //try update again.
    }
  }

  public boolean delete() throws DatabaseDeleteException{
    return cacheStorage.delete(queryId) && databaseStorage.delete(queryId);
  }
}