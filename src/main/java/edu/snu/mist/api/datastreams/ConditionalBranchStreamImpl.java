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

package edu.snu.mist.api.datastreams;

import edu.snu.mist.api.datastreams.configurations.DummyOperatorConfiguration;
import edu.snu.mist.common.graph.DAG;
import edu.snu.mist.common.graph.MISTEdge;
import edu.snu.mist.formats.avro.Direction;
import org.apache.reef.tang.Configuration;

/**
 * This class implements ConditionalBranchStream.
 * <T> data type of the stream.
 */
final class ConditionalBranchStreamImpl<T> extends MISTStreamImpl<T> implements ConditionalBranchStream<T> {

  ConditionalBranchStreamImpl(final DAG<MISTStream, MISTEdge> dag,
                              final Configuration conf) {
    super(dag, conf);
  }

  @Override
  public ContinuousStream<T> branch(final int index) {
    final Configuration opConf = DummyOperatorConfiguration.CONF.build();
    final ContinuousStream<T> downStream = new ContinuousStreamImpl<>(dag, opConf);
    dag.addVertex(downStream);
    dag.addEdge(this, downStream, new MISTEdge(Direction.LEFT, index));
    return downStream;
  }
}
