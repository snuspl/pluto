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
import edu.snu.mist.task.executor.parameters.MistExecutorId;
import edu.snu.mist.task.parameters.NumExecutors;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.exceptions.InjectionException;

import javax.inject.Inject;
import java.util.LinkedList;
import java.util.List;

/**
 * This returns a list of executors.
 */
final class DefaultExecutorListProviderImpl implements ExecutorListProvider {

  /**
   * A list of executors.
   */
  private final List<MistExecutor> executors;

  /**
   * @param numExecutors the number of MistExecutors
   * @throws InjectionException
   */
  @Inject
  private DefaultExecutorListProviderImpl(
      @Parameter(NumExecutors.class) final int numExecutors) throws InjectionException {
    this.executors = new LinkedList<>();
    for (int i = 0; i < numExecutors; i++) {
      final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
      jcb.bindNamedParameter(MistExecutorId.class, "MistExecutor-" + i);
      final Injector injector = Tang.Factory.getTang().newInjector(jcb.build());
      final MistExecutor executor = injector.getInstance(MistExecutor.class);
      this.executors.add(executor);
    }
  }

  @Override
  public List<MistExecutor> getExecutors() {
    return executors;
  }
}
