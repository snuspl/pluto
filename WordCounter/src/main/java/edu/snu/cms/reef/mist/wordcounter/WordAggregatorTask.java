/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package edu.snu.cms.reef.mist.wordcounter;

import org.apache.reef.io.network.Message;
import org.apache.reef.io.network.NetworkConnectionService;
import org.apache.reef.io.network.impl.config.NetworkConnectionServiceIdFactory;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.task.Task;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.Identifier;
import org.apache.reef.wake.IdentifierFactory;
import org.apache.reef.wake.remote.impl.StringCodec;

import javax.inject.Inject;
import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.HashMap;
import java.util.Map;

/**
 * A 'WordAggregator' Task.
 */
public final class WordAggregatorTask implements Task {

  private Map<String, Integer> counts = new HashMap<String, Integer>();
  private int count = 0;

  @NamedParameter
  public static class ReceiverName implements Name<String> {
  }

  private static final Logger LOG = Logger.getLogger(WordAggregatorTask.class.getName());

  private String[] splitter(final String sentence) {
    String[] words = sentence.split(" ");
    return words;
  }

  private void counter(final String word) {
    if(counts.containsKey(word)) {
      counts.put(word, counts.get(word)+1);
    } else {
      counts.put(word, 1);
    }
  }

  private class StringMessageHandler implements EventHandler<Message<String>> {
    @Override
    public void onNext(final Message<String> message) {
      final Iterator<String> iter = message.getData().iterator();
      while(iter.hasNext()) {
        count++;
        String sentence = iter.next();
        String[] words = splitter(sentence);
        for(String word : words) {
          counter(word);
        }
        System.out.println("Count = " + count);
        for(Map.Entry<String, Integer> item : counts.entrySet()) {
          System.out.println("Word: " + item.getKey() + ", Count: " + item.getValue());
        }
      }
    }
  }

  @Inject
  private WordAggregatorTask(final NetworkConnectionService ncs,
                             @Parameter(ReceiverName.class) final String receiverName)
      throws InjectionException {
    final Injector injector = Tang.Factory.getTang().newInjector();
    final IdentifierFactory idFac = injector.getNamedInstance(NetworkConnectionServiceIdFactory.class);
    final Identifier connId = idFac.getNewInstance("connection");
    final Identifier receiverId = idFac.getNewInstance(receiverName);
    ncs.registerConnectionFactory(connId, new StringCodec(), new StringMessageHandler(),
        new WordCounterLinkListener(), receiverId);
    LOG.log(Level.FINE, "Receiver Task Started");
  }

  @Override
  public byte[] call(final byte[] memento) {
    while(true) {
      // TODO: Sleep or wait instead of spin
    }
  }
}

