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

import edu.snu.mist.common.NettyDataStreamPublisher;
import edu.snu.mist.task.sources.NettyTextSourceFactory;
import edu.snu.mist.task.sources.Source;
import edu.snu.mist.task.sources.TextSourceFactory;
import junit.framework.Assert;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;

public final class TextSourceFactoryTest {

  private static final String QUERY_ID = "testQuery";
  private static final String SERVER_ADDR = "localhost";
  private static final int SERVER_PORT = 12112;

  /**
   * Test whether the created sources by NettyTextSourceFactory receive data stream correctly from netty server.
   * It creates a netty source server and 4 receivers.
   * @throws Exception
   */
  @Test(timeout = 10000L)
  public void testNettyTextSourceFactory() throws Exception {
    // create netty server
    final NettyDataStreamPublisher streamPublisher = new NettyDataStreamPublisher(SERVER_ADDR, SERVER_PORT);
    final Injector injector = Tang.Factory.getTang().newInjector();

    // create netty sources
    final TextSourceFactory textSourceFactory = injector.getInstance(NettyTextSourceFactory.class);
    final Source<String> source1 = textSourceFactory.newInstance(QUERY_ID, "1", SERVER_ADDR, SERVER_PORT);
    final Source<String> source2 = textSourceFactory.newInstance(QUERY_ID, "2", SERVER_ADDR, SERVER_PORT);
    final Source<String> source3 = textSourceFactory.newInstance(QUERY_ID, "3", SERVER_ADDR, SERVER_PORT);
    final Source<String> source4 = textSourceFactory.newInstance(QUERY_ID, "4", SERVER_ADDR, SERVER_PORT);

    // set output emitter
    final AtomicBoolean received1 = new AtomicBoolean(false);
    final AtomicBoolean received2 = new AtomicBoolean(false);
    final AtomicBoolean received3 = new AtomicBoolean(false);
    final AtomicBoolean received4 = new AtomicBoolean(false);
    source1.setOutputEmitter((output) -> received1.compareAndSet(false, true));
    source2.setOutputEmitter((output) -> received2.compareAndSet(false, true));
    source3.setOutputEmitter((output) -> received3.compareAndSet(false, true));
    source4.setOutputEmitter((output) -> received3.compareAndSet(false, true));

    // start to receive stream
    source1.start();
    source2.start();
    source3.start();

    while (!(received1.get() && received2.get() && received3.get())) {
      streamPublisher.write("hello world!\n");
    }
    Assert.assertEquals(false, received4.get());
  }
}
