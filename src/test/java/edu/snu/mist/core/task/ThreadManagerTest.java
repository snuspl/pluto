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

import edu.snu.mist.common.MistEvent;
import edu.snu.mist.common.OutputEmitter;
import edu.snu.mist.core.parameters.NumThreads;
import edu.snu.mist.formats.avro.Direction;
import junit.framework.Assert;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Set;

import static java.lang.Thread.sleep;

public class ThreadManagerTest {

  private DynamicThreadManager threadManager;
  private static final int DEFAULT_NUM_THREADS = 5;
  private static final int MAX_NUM_THREADS = 10;

  @Before
  public void setUp() throws InjectionException {
    final BlockingActiveOperatorChainPickManager operatorChainManager =
        Tang.Factory.getTang().newInjector().getInstance(BlockingActiveOperatorChainPickManager.class);
    for (int i = 0; i < MAX_NUM_THREADS + 1; i++) {
      final OperatorChain operatorChain = new TestOperatorChain(operatorChainManager);
      operatorChainManager.insert(operatorChain);
    }

    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    jcb.bindNamedParameter(NumThreads.class, Integer.toString(DEFAULT_NUM_THREADS));
    final Injector injector = Tang.Factory.getTang().newInjector(jcb.build());
    injector.bindVolatileInstance(
        BlockingActiveOperatorChainPickManager.class, operatorChainManager);
    threadManager = injector.getInstance(DynamicThreadManager.class);
  }

  @After
  public void tearDown() throws Exception {
    threadManager.close();
  }


  /**
   * Test whether DynamicThreadManager creates fixed number of threads correctly.
   */
  @Test
  public void testFixedNumberThreadGeneration() throws Exception {
    final Set<EventProcessor> threads = threadManager.getEventProcessors();
    Assert.assertEquals(threads.size(), DEFAULT_NUM_THREADS);
  }

  /**
   * Test whether DynamicThreadManager generates accurate number of processors synchronously or not.
   */
  @Test
  public void testDynamicThreadGeneration() throws Exception {
    // The event processors will be generated synchronously.
    threadManager.setThreadNum(MAX_NUM_THREADS);

    final Set<EventProcessor> threads = threadManager.getEventProcessors();
    Assert.assertEquals(MAX_NUM_THREADS, threads.size());
    threadManager.close();
  }

  /**
   * Test whether DynamicThreadManager reaps accurate number of processors asynchronously or not.
   */
  @Test
  public void testDynamicThreadReaping() throws Exception {
    // The event processors will be reaped asynchronously.
    threadManager.setThreadNum(2);

    sleep(100);
    final Set<EventProcessor> eventProcessors = threadManager.getEventProcessors();
    Assert.assertEquals(2, eventProcessors.size());
    threadManager.close();
  }

  /**
   * The operator chain for test.
   * If some processor process this chain, it just re-insert itself into the manager's queue.
   */
  private final class TestOperatorChain implements OperatorChain {

    private final OperatorChainManager manager;

    TestOperatorChain(final OperatorChainManager manager) {
      this.manager = manager;
    }

    @Override
    public void insertToHead(final PhysicalOperator newOperator) {
      // do nothing
    }

    @Override
    public void insertToTail(final PhysicalOperator newOperator) {
      // do nothing
    }

    @Override
    public PhysicalOperator removeFromTail() {
      return null;
    }

    @Override
    public PhysicalOperator removeFromHead() {
      return null;
    }

    @Override
    public boolean processNextEvent() {
      // insert again
      manager.insert(this);
      return true;
    }

    @Override
    public boolean addNextEvent(final MistEvent event, final Direction direction) {
      return true;
    }

    @Override
    public int size() {
      return 0;
    }

    @Override
    public int numberOfEvents() {
      return 0;
    }

    @Override
    public PhysicalOperator get(final int index) {
      return null;
    }

    @Override
    public void setOperatorChainManager(final OperatorChainManager operatorChainManager) {
      // do nothing
    }

    @Override
    public Type getType() {
      return Type.OPERATOR_CHIAN;
    }

    @Override
    public void setOutputEmitter(final OutputEmitter emitter) {
      // do nothing
    }
  }
}
