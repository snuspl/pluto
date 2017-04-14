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

import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.Assert;
import org.junit.Test;

public class OperatorChainManagerTest {

  /**
   * Test whether RandomlyPickManager selects an operator chain.
   */
  @Test
  public void randomPickManagerTest() throws InjectionException {
    final Injector injector = Tang.Factory.getTang().newInjector();
    final NonBlockingRandomlyPickManager operatorChainManager
        = injector.getInstance(NonBlockingRandomlyPickManager.class);

    // Select a chain
    final OperatorChain chain = new DefaultOperatorChainImpl();
    operatorChainManager.insert(chain);
    final OperatorChain selectedChain = operatorChainManager.pickOperatorChain();
    Assert.assertEquals(chain, selectedChain);

    // When QueryPickManager has no chain, it should returns null
    operatorChainManager.delete(chain);
    final OperatorChain c = operatorChainManager.pickOperatorChain();
    Assert.assertEquals(null, c);
  }

  @Test
  public void nonBlockingActiveQueryPickManagerTest() throws InjectionException {
    final Injector injector = Tang.Factory.getTang().newInjector();
    final NonBlockingActiveOperatorChainPickManager operatorChainManager
        = injector.getInstance(NonBlockingActiveOperatorChainPickManager.class);

    // Select a chain
    final OperatorChain chain = new DefaultOperatorChainImpl();
    operatorChainManager.insert(chain);
    final OperatorChain selectedChain = operatorChainManager.pickOperatorChain();
    Assert.assertEquals(chain, selectedChain);

    // When QueryPickManager has no chain, it should returns null
    operatorChainManager.delete(chain);
    final OperatorChain c = operatorChainManager.pickOperatorChain();
    Assert.assertEquals(null, c);
  }

  @Test
  public void blockingActiveQueryPickManagerTest() throws InjectionException, InterruptedException {
    final Injector injector = Tang.Factory.getTang().newInjector();
    final BlockingActiveOperatorChainPickManager operatorChainManager
        = injector.getInstance(BlockingActiveOperatorChainPickManager.class);

    // Select a chain
    final OperatorChain chain = new DefaultOperatorChainImpl();
    operatorChainManager.insert(chain);
    final OperatorChain selectedChain = operatorChainManager.pickOperatorChain();
    Assert.assertEquals(chain, selectedChain);
  }
}