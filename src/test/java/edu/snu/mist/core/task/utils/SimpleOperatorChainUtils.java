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
package edu.snu.mist.core.task.utils;

import edu.snu.mist.common.MistDataEvent;
import edu.snu.mist.common.MistEvent;
import edu.snu.mist.common.OutputEmitter;
import edu.snu.mist.common.operators.FilterOperator;
import edu.snu.mist.common.operators.UnionOperator;
import edu.snu.mist.common.sinks.Sink;
import edu.snu.mist.core.task.*;

/**
 * This is a utility class for simple operator chain generation.
 */
public final class SimpleOperatorChainUtils {

  private SimpleOperatorChainUtils() {
    // empty constructor
  }

  /**
   * Generate a simple source for test.
   * @return test source
   */
  public static PhysicalSource generateTestSource(final IdAndConfGenerator idAndConfGenerator) {
    return new TestSource(idAndConfGenerator.generateId(), idAndConfGenerator.generateConf());
  }

  /**
   * Generate a simple sink for test.
   * @return test sink
   */
  public static PhysicalSink generateTestSink(final IdAndConfGenerator idAndConfGenerator) {
    return new TestSink(idAndConfGenerator.generateId(), idAndConfGenerator.generateConf());
  }

  /**
   * Generate a simple operator chain that has a filter operator.
   * @return operator chain
   */
  public static OperatorChain generateFilterOperatorChain(final IdAndConfGenerator idAndConfGenerator) {
    final OperatorChain operatorChain = new DefaultOperatorChainImpl();
    final PhysicalOperator filterOp = new DefaultPhysicalOperatorImpl(idAndConfGenerator.generateId(),
        idAndConfGenerator.generateConf(), new FilterOperator<>((input) -> true), operatorChain);
    operatorChain.insertToHead(filterOp);
    return operatorChain;
  }

  /**
   * Generate a simple operator chain that has a union operator.
   * @return operator chain
   */
  public static OperatorChain generateUnionOperatorChain(final IdAndConfGenerator idAndConfGenerator) {
    final OperatorChain operatorChain = new DefaultOperatorChainImpl();
    final PhysicalOperator filterOp = new DefaultPhysicalOperatorImpl(idAndConfGenerator.generateId(),
        idAndConfGenerator.generateConf(), new UnionOperator(), operatorChain);
    operatorChain.insertToHead(filterOp);
    return operatorChain;
  }

  /**
   * Generate a Mist data event for test.
   * @return mist data event
   */
  public static MistEvent generateTestEvent() {
    return new MistDataEvent("Test");
  }

  /**
   * Test source that doesn't send any data actually.
   */
  private static final class TestSource implements PhysicalSource {
    private final String id;
    private final String conf;

    private TestSource(final String id,
                       final String conf) {
      this.id = id;
      this.conf = conf;
    }

    @Override
    public void start() {
      // do nothing
    }

    @Override
    public void close() throws Exception {
      // do nothing
    }

    @Override
    public Type getType() {
      return Type.SOURCE;
    }

    @Override
    public void setOutputEmitter(final OutputEmitter emitter) {
      // do nothing
    }

    @Override
    public String getId() {
      return id;
    }

    @Override
    public String getConfiguration() {
      return conf;
    }
  }

  /**
   * Test sink that doesn't process any data actually.
   */
  private static final class TestSink implements PhysicalSink {

    private final String id;
    private final String conf;

    private TestSink(final String id,
                    final String conf) {
      this.id = id;
      this.conf = conf;
    }

    @Override
    public Type getType() {
      return Type.SINK;
    }

    @Override
    public String getId() {
      return id;
    }

    @Override
    public String getConfiguration() {
      return conf;
    }

    @Override
    public Sink getSink() {
      return null;
    }
  }
}
