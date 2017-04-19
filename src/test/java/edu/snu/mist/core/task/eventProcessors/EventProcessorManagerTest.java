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
package edu.snu.mist.core.task.eventProcessors;

import edu.snu.mist.core.task.eventProcessors.parameters.NumEventProcessors;
import junit.framework.Assert;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.inject.Inject;
import java.util.Set;

import static java.lang.Thread.sleep;
import static org.mockito.Mockito.mock;

public class EventProcessorManagerTest {

  private EventProcessorManager eventProcessorManager;
  private static final int DEFAULT_NUM_THREADS = 5;
  private static final int MAX_NUM_THREADS = 10;

  @Before
  public void setUp() throws InjectionException {
    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    jcb.bindNamedParameter(NumEventProcessors.class, Integer.toString(DEFAULT_NUM_THREADS));
    jcb.bindImplementation(EventProcessorFactory.class, TestEventProcessorFactory.class);
    final Injector injector = Tang.Factory.getTang().newInjector(jcb.build());
    eventProcessorManager = injector.getInstance(DefaultEventProcessorManager.class);
  }

  @After
  public void tearDown() throws Exception {
    eventProcessorManager.close();
  }

  /**
   * Test whether EventProcessorManager creates fixed number of event processors correctly.
   */
  @Test
  public void testFixedNumberGeneration() throws Exception {
    final Set<EventProcessor> eventProcessors = eventProcessorManager.getEventProcessors();
    Assert.assertEquals(eventProcessors.size(), DEFAULT_NUM_THREADS);
  }

  /**
   * Test whether EventProcessorManager creates new event processors when current_num_processors < adjust_num.
   */
  @Test
  public void testIncreaseEventProcessors() throws Exception {
    // The event processors will be generated synchronously.
    eventProcessorManager.adjustEventProcessorNum(MAX_NUM_THREADS);

    final Set<EventProcessor> eventProcessors = eventProcessorManager.getEventProcessors();
    Assert.assertEquals(MAX_NUM_THREADS, eventProcessors.size());
    eventProcessorManager.close();
  }

  /**
   * Test whether EventProcessorManager remove existing event processors when current_num_processors > adjust_num.
   */
  @Test
  public void testDecreaseEventProcessors() throws Exception {
    // The event processors will be deleted asynchronously.
    eventProcessorManager.adjustEventProcessorNum(2);

    sleep(100);
    final Set<EventProcessor> eventProcessors = eventProcessorManager.getEventProcessors();
    Assert.assertEquals(2, eventProcessors.size());
    eventProcessorManager.close();
  }

  /**
   * This is a mock event processor factory that creates mock event processors.
   */
  static final class TestEventProcessorFactory implements EventProcessorFactory {

    @Inject
    private TestEventProcessorFactory() {
      // empty
    }

    @Override
    public EventProcessor newEventProcessor() {
      return mock(EventProcessor.class);
    }
  }
}
