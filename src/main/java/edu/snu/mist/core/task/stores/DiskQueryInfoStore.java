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
package edu.snu.mist.core.task.stores;


import edu.snu.mist.core.parameters.TempFolderPath;
import edu.snu.mist.formats.avro.LogicalPlan;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.reef.io.Tuple;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.List;


/**
 * It saves the information of a query (logical plan, jar files) into the disk.
 */

final class DiskQueryInfoStore implements QueryInfoStore {
  /**
   * A path for the temporary folder that stores jar files and logical plans of queries.
   */
  private final String tmpFolderPath;

  /**
   * A writer that stores logical plans.
   */
  private final DatumWriter<LogicalPlan> datumWriter;

  /**
   * A reader that reads stored logical plans.
   */
  private final DatumReader<LogicalPlan> datumReader;

  /**
   * A file name generator that generates jar file's names.
   */
  private final FileNameGenerator fileNameGenerator;

  @Inject
  private DiskQueryInfoStore(@Parameter(TempFolderPath.class) final String tmpFolderPath,
                             final FileNameGenerator fileNameGenerator) {
    this.tmpFolderPath = tmpFolderPath;
    this.fileNameGenerator = fileNameGenerator;
    this.datumWriter = new SpecificDatumWriter<>(LogicalPlan.class);
    this.datumReader = new SpecificDatumReader<>(LogicalPlan.class);
    // Create a folder that stores logical plans and jar files
    final File folder = new File(tmpFolderPath);
    if (!folder.exists()) {
      folder.mkdir();
    } else {
      final File[] destroy = folder.listFiles();
      for (final File des : destroy) {
        des.delete();
      }
    }
  }

  /**
   * Gets a file of the stored logical plan corresponding to the queryId.
   * @param queryId query id for the logical plan
   * @return file of the stored logical plan
   */
  private File getLogicalPlanFile(final String queryId) {
    final StringBuilder sb = new StringBuilder(queryId);
    sb.append(".plan");
    return new File(tmpFolderPath, sb.toString());
  }

  /**
   * Saves the logical plan as queryId.plan to disk.
   * @param tuple
   * @throws IOException
   */
  @Override
  public boolean savePlan(final Tuple<String, LogicalPlan> tuple) throws IOException {
    final LogicalPlan plan = tuple.getValue();
    final File storedPlanFile = getLogicalPlanFile(tuple.getKey());
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
   * Saves the serialized jar files into the disk.
   * This generates file names for the jar files from fileNameGenerator.
   * @param jarFiles jar files
   * @return paths of the stored jar files
   * @throws IOException throws an exception when the jar file is not able to be saved.
   */
  @Override
  public List<String> saveJar(final List<ByteBuffer> jarFiles) throws IOException {
    final List<String> paths = new LinkedList<>();
    for (final ByteBuffer jarFileBytes : jarFiles) {
      final String path = String.format("submitted-%s.jar", fileNameGenerator.generate());
      final Path jarFilePath = Paths.get(tmpFolderPath, path);
      final File jarFile = jarFilePath.toFile();
      final FileChannel wChannel = new FileOutputStream(jarFile, false).getChannel();
      wChannel.write(jarFileBytes);
      wChannel.close();
      paths.add(jarFile.getAbsolutePath());
    }
    return paths;
  }

  /**
   * Load the stored logical plan from File.
   * @param storedPlanFile file
   * @return logical plan
   * @throws IOException
   */
  private LogicalPlan loadFromFile(final File storedPlanFile) throws IOException {
    final DataFileReader<LogicalPlan> dataFileReader = new DataFileReader<LogicalPlan>(storedPlanFile, datumReader);
    LogicalPlan plan = null;
    plan = dataFileReader.next(plan);
    return plan;
  }

  /**
   * Loads the logical plan, queryId.plan, from disk.
   * @param queryId
   * @return Logical plan corresponding to queryId
   * @throws IOException
   */
  @Override
  public LogicalPlan load(final String queryId) throws IOException {
    final File storedPlanFile = getLogicalPlanFile(queryId);
    return loadFromFile(storedPlanFile);
  }

  /**
   * Deletes the logical plan and jar files.
   * @param queryId
   * @throws IOException
   */
  @Override
  public void delete(final String queryId) throws IOException {
    final File storedPlanFile = getLogicalPlanFile(queryId);
    final LogicalPlan logicalPlan = loadFromFile(storedPlanFile);
    final List<String> paths = logicalPlan.getJarFilePaths();

    // Delete jar files for the query
    for (final String path : paths) {
      if (path.startsWith(tmpFolderPath)) {
        // Delete the jar file if it is in the temp folder
        final File jarFile = new File(path);
        if (jarFile.exists()) {
          jarFile.delete();
        }
      }
    }

    // Delete the logical plan
    if (storedPlanFile.exists()) {
      storedPlanFile.delete();
    }
  }
}
