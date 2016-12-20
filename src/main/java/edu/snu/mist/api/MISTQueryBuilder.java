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
package edu.snu.mist.api;

import edu.snu.mist.api.sources.TextSocketSourceStream;
import edu.snu.mist.api.sources.builder.PeriodicWatermarkConfiguration;
import edu.snu.mist.api.sources.builder.TextSocketSourceConfiguration;
import edu.snu.mist.api.sources.builder.WatermarkConfiguration;
import edu.snu.mist.common.AdjacentListDAG;
import edu.snu.mist.common.DAG;
import edu.snu.mist.formats.avro.Direction;

/**
 * This class builds MIST query.
 */
public final class MISTQueryBuilder {

  /**
   * DAG of the query.
   */
  private final DAG<AvroVertexSerializable, Direction> dag;

  /**
   * The default watermark configuration.
   */
  private final PeriodicWatermarkConfiguration<String> defaultWatermarkConf =
      PeriodicWatermarkConfiguration.<String>newBuilder()
          .setWatermarkPeriod(100)
          .setExpectedDelay(0)
          .build();

  public MISTQueryBuilder() {
    this.dag = new AdjacentListDAG<>();
  }

  /**
   * Get socket text stream.
   * @param sourceConf socket text source
   * @return source stream
   */
  public TextSocketSourceStream socketTextStream(final TextSocketSourceConfiguration sourceConf) {
    return socketTextStream(sourceConf, defaultWatermarkConf);
  }

  /**
   * Get socket text stream.
   * @param sourceConf socket text source
   * @param watermarkConf watermark configuration
   * @return source stream
   */
  public TextSocketSourceStream socketTextStream(final TextSocketSourceConfiguration sourceConf,
                                                 final WatermarkConfiguration<String> watermarkConf) {
    final TextSocketSourceStream sourceStream = new TextSocketSourceStream(sourceConf, dag, watermarkConf);
    dag.addVertex(sourceStream);
    return sourceStream;
  }

  /**
   * Build the query.
   * @return the query
   */
  public MISTQuery build() {
    return new MISTQueryImpl(dag);
  }
}
