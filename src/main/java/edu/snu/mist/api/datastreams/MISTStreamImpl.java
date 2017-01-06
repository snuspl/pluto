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

import edu.snu.mist.common.DAG;
import edu.snu.mist.common.exceptions.IllegalUdfConfigurationException;
import edu.snu.mist.formats.avro.Direction;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;

/**
 * The basic implementation class for MISTStream.
 */
class MISTStreamImpl<OUT> implements MISTStream<OUT> {

  /**
   * DAG of the query.
   */
  protected final DAG<MISTStream, Direction> dag;
  /**
   * Configuration of the stream.
   */
  protected final Configuration conf;

  public MISTStreamImpl(final DAG<MISTStream, Direction> dag,
                        final Configuration conf) {
    this.dag = dag;
    this.conf = conf;
  }


  /**
   * This function creates the instance of the udf function
   * in order to check whether the configuration is correct or not.
   * @param clazz class of the udf function
   * @param udfConf configuration of the udf function
   * @param <C> class type
   */
  protected <C> void checkUdf(final Class<? extends C> clazz,
                              final Configuration udfConf) {
    final Injector injector = Tang.Factory.getTang().newInjector(udfConf);
    try {
      final C instance = injector.getInstance(clazz);
    } catch (final InjectionException e) {
      // It will throw an InjectionException if the configuration is wrong.
      e.printStackTrace();
      throw new IllegalUdfConfigurationException(e);
    }
  }


  /**
   * Transform the upstream to a new continuous stream
   * by applying the operation corresponding to the given configuration.
   * @param opConf configuration
   * @param upStream upstream
   * @param <OUT> output type
   * @return continuous stream
   */
  protected <OUT> ContinuousStream<OUT> transformToSingleInputContinuousStream(
      final Configuration opConf,
      final MISTStream upStream) {
    final ContinuousStream<OUT> downStream = new ContinuousStreamImpl<>(dag, opConf);
    dag.addVertex(downStream);
    dag.addEdge(upStream, downStream, Direction.LEFT);
    return downStream;
  }

  @Override
  public Configuration getConfiguration() {
    return conf;
  }
}
