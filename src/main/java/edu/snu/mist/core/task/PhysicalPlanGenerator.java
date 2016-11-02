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
package edu.snu.mist.core.task;

import edu.snu.mist.api.StreamType;
import edu.snu.mist.common.DAG;
import edu.snu.mist.formats.avro.LogicalPlan;
import edu.snu.mist.core.task.common.PhysicalVertex;
import org.apache.reef.io.Tuple;
import org.apache.reef.tang.annotations.DefaultImplementation;

import java.io.IOException;

/**
 * This interface is for generating physical plan from logical plan.
 * This deserializes the logical plan and creates physical plan.
 */
@DefaultImplementation(DefaultPhysicalPlanGeneratorImpl.class)
public interface PhysicalPlanGenerator {
  /**
   * Generates the physical plan by deserializing the logical plan.
   * @param queryIdAndLogicalPlan the tuple of queryId and logical plan
   * @return physical plan
   */
  DAG<PhysicalVertex, StreamType.Direction> generate(Tuple<String, LogicalPlan> queryIdAndLogicalPlan)
      throws IOException, ClassNotFoundException;
}
