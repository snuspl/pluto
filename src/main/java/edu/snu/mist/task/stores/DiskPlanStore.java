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
package edu.snu.mist.task.stores;


import edu.snu.mist.formats.avro.LogicalPlan;
import edu.snu.mist.task.parameters.PlanStorePath;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.*;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.reef.io.Tuple;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.io.File;
import java.io.IOException;


/**
 * Default implementation of PlanStore.
 * It saves the plan into a disk and
 * loads the plan from a disk.
 */
final class DiskPlanStore implements PlanStore {
  private final String planStorePath;
  private final DatumWriter<LogicalPlan> datumWriter;
  private final DatumReader<LogicalPlan> datumReader;

  @Inject
  private DiskPlanStore(@Parameter(PlanStorePath.class) final String planStorePath) {
    this.planStorePath = planStorePath;
    this.datumWriter = new SpecificDatumWriter<LogicalPlan>(LogicalPlan.class);
    this.datumReader = new SpecificDatumReader<LogicalPlan>(LogicalPlan.class);
    final File planFolder = new File(planStorePath);
    if (!planFolder.exists()) {
      planFolder.mkdir();
    } else {
      final File[] destroy = planFolder.listFiles();
      for (final File des : destroy) {
        des.delete();
      }
    }
  }

  /**
   * Saves the logical plan as queryId.plan to disk.
   * @param tuple
   * @throws IOException
   */
  @Override
  public boolean save(final Tuple<String, LogicalPlan> tuple) throws IOException {
    final LogicalPlan plan = tuple.getValue();
    final String queryId = tuple.getKey();
    final File storedPlanFile= new File(planStorePath, queryId + ".plan");
    if (!storedPlanFile.exists()) {
      final DataFileWriter<LogicalPlan> dataFileWriter = new DataFileWriter<LogicalPlan>(datumWriter);
      dataFileWriter.create(plan.getSchema(), storedPlanFile);
      dataFileWriter.append(plan);
      dataFileWriter.close();
      return true;
    }
    return false;
  }

  /**
   * Loads the logical plan, queryId.plan, from disk.
   * @param queryId
   * @return Logical plan corresponding to queryId
   * @throws IOException
   */
  @Override
  public LogicalPlan load(final String queryId) throws IOException {
    final File storedPlanFile = new File(planStorePath, queryId + ".plan");
    final DataFileReader<LogicalPlan> dataFileReader = new DataFileReader<LogicalPlan>(storedPlanFile, datumReader);
    LogicalPlan plan = null;
    plan = dataFileReader.next(plan);
    return plan;
  }

  /**
   * Deletes the logical plan from disk.
   * @param queryId
   * @throws IOException
   */
  @Override
  public void delete(final String queryId) throws IOException {
    final File storedPlanFile = new File(planStorePath, queryId + ".plan");
    if (storedPlanFile.exists()) {
      storedPlanFile.delete();
    }
  }
}
