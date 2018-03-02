/*
 * Copyright (C) 2018 Seoul National University
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
import edu.snu.mist.formats.avro.ApplicationInfoCheckpoint;
import edu.snu.mist.formats.avro.CheckpointResult;
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
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

public final class DefaultAppInfoCheckpointStore implements AppInfoCheckpointStore {

  private static final Logger LOG = Logger.getLogger(DefaultAppInfoCheckpointStore.class.getName());

  private final String tmpFolderPath;

  /**
   * A writer that stores ApplicationInfoCheckpoint.
   */
  private final DatumWriter<ApplicationInfoCheckpoint> datumWriter;

  /**
   * A reader that reads stored ApplicationInfoCheckpoint.
   */
  private final DatumReader<ApplicationInfoCheckpoint> datumReader;

  @Inject
  private DefaultAppInfoCheckpointStore(@Parameter(TempFolderPath.class) final String tmpFolderpath) {
    this.tmpFolderPath = tmpFolderpath;
    this.datumWriter = new SpecificDatumWriter<>(ApplicationInfoCheckpoint.class);
    this.datumReader = new SpecificDatumReader<>(ApplicationInfoCheckpoint.class);
    // Create a folder that stores the dags and jar files
    final File folder = new File(tmpFolderPath);
    if (!folder.exists()) {
      folder.mkdir();
    } else {
      // TODO : Should not delete other checkpoints.
      final File[] destroy = folder.listFiles();
      for (final File des : destroy) {
        des.delete();
      }
    }
  }

  private File getAppInfoCheckpointFile(final String groupId) {
    final StringBuilder sb = new StringBuilder(groupId);
    sb.append(".groupcp");
    return new File(tmpFolderPath, sb.toString());
  }


  @Override
  public CheckpointResult saveMetaGroupCheckpoint(final Tuple<String, ApplicationInfoCheckpoint> tuple) {
    try {
      final String groupId = tuple.getKey();
      final ApplicationInfoCheckpoint gmc = tuple.getValue();

      // Write the file.
      final File storedFile = getAppInfoCheckpointFile(groupId);
      if (storedFile.exists()) {
        storedFile.delete();
        LOG.log(Level.INFO, "Checkpoint deleted for groupId: {0}", groupId);
      }
      if (!storedFile.exists()) {
        storedFile.getParentFile().mkdirs();
        final DataFileWriter<ApplicationInfoCheckpoint> dataFileWriter = new DataFileWriter<>(datumWriter);
        dataFileWriter.create(gmc.getSchema(), storedFile);
        dataFileWriter.append(gmc);
        dataFileWriter.close();
        LOG.log(Level.INFO, "Checkpoint completed for groupId: {0}", groupId);
      }
    } catch (final Exception e) {
      e.printStackTrace();
      return CheckpointResult.newBuilder()
          .setIsSuccess(false)
          .setMsg("Unsuccessful in checkpointing group " + tuple.getKey())
          .setPathToCheckpoint("")
          .build();
    }
    return CheckpointResult.newBuilder()
        .setIsSuccess(true)
        .setMsg("Successfully checkpointed group " + tuple.getKey())
        .setPathToCheckpoint(tmpFolderPath + "/" + tuple.getKey() + ".groupcp")
        .build();
  }

  @Override
  public ApplicationInfoCheckpoint loadAppInfoCheckpoint(final String groupId) throws IOException {
    // Load the file.
    final File storedFile = getAppInfoCheckpointFile(groupId);
    final DataFileReader<ApplicationInfoCheckpoint> dataFileReader =
        new DataFileReader<ApplicationInfoCheckpoint>(storedFile, datumReader);
    ApplicationInfoCheckpoint mgc = null;
    mgc = dataFileReader.next(mgc);
    if (mgc != null) {
      LOG.log(Level.INFO, "Checkpoint file found. groupId is " + groupId);
    } else {
      LOG.log(Level.WARNING, "Checkpoint file not found or error during loading. groupId is " + groupId);
    }
    return mgc;
  }
}
