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

import edu.snu.mist.core.operators.Operator;
import edu.snu.mist.core.operators.StateHandler;
import edu.snu.mist.core.parameters.SharedStorePath;
import edu.snu.mist.core.task.DefaultPhysicalOperatorImpl;
import edu.snu.mist.core.task.ExecutionDag;
import edu.snu.mist.core.task.ExecutionVertex;
import edu.snu.mist.core.task.groupaware.Group;
import edu.snu.mist.formats.avro.AvroDag;
import edu.snu.mist.formats.avro.CheckpointResult;
import edu.snu.mist.formats.avro.GroupCheckpoint;
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
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public final class DefaultGroupCheckpointStore implements GroupCheckpointStore {

  private static final Logger LOG = Logger.getLogger(DefaultGroupCheckpointStore.class.getName());

  private final String tmpFolderPath;

  /**
   * A writer that stores avro dag datum writer.
   */
  private final DatumWriter<AvroDag> avroDagDatumWriter;

  /**
   * A reader that reads stored avro dags.
   */
  private final DatumReader<AvroDag> avroDagDatumReader;

  /**
   * A writer that stores ApplicationInfoCheckpoint.
   */
  private final DatumWriter<GroupCheckpoint> groupCheckpointDatumWriter;

  /**
   * A reader that reads stored ApplicationInfoCheckpoint.
   */
  private final DatumReader<GroupCheckpoint> groupCheckpointDatumReader;

  @Inject
  private DefaultGroupCheckpointStore(@Parameter(SharedStorePath.class) final String tmpFolderpath) {
    this.tmpFolderPath = tmpFolderpath;
    this.avroDagDatumWriter = new SpecificDatumWriter<>(AvroDag.class);
    this.avroDagDatumReader = new SpecificDatumReader<>(AvroDag.class);
    this.groupCheckpointDatumWriter = new SpecificDatumWriter<>(GroupCheckpoint.class);
    this.groupCheckpointDatumReader = new SpecificDatumReader<>(GroupCheckpoint.class);
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

  private File getGroupCheckpointFile(final String groupId) {
    final StringBuilder sb = new StringBuilder(groupId);
    sb.append(".checkpoint");
    return new File(tmpFolderPath, sb.toString());
  }

  private File getQueryStoreFile(final String queryId) {
    final StringBuilder sb = new StringBuilder(queryId);
    sb.append(".query");
    return new File(tmpFolderPath, sb.toString());
  }

  @Override
  public boolean saveQuery(final AvroDag avroDag) {
    final String queryId = avroDag.getQueryId();
    try {
      final File storedFile = getQueryStoreFile(queryId);
      if (storedFile.exists()) {
        storedFile.delete();
        LOG.log(Level.INFO, "Deleting a duplicate query file");
      } else {
        storedFile.getParentFile().mkdirs();
        final DataFileWriter<AvroDag> dataFileWriter = new DataFileWriter<>(avroDagDatumWriter);
        dataFileWriter.create(avroDag.getSchema(), storedFile);
        dataFileWriter.append(avroDag);
        dataFileWriter.close();
        LOG.log(Level.INFO, "Query {0} has been stored to disk.", queryId);
      }
      return true;
    } catch (final Exception e) {
      e.printStackTrace();
      return false;
    }
  }

  @Override
  public CheckpointResult checkpointGroupStates(final Tuple<String, Group> tuple) {
    final String groupId = tuple.getKey();
    final Group group = tuple.getValue();
    final GroupCheckpoint checkpoint = group.checkpoint();
    try {
      // Write the file.
      final File storedFile = getGroupCheckpointFile(groupId);
      if (storedFile.exists()) {
        storedFile.delete();
        LOG.log(Level.INFO, "Checkpoint deleted for groupId: {0}", groupId);
      }
      if (!storedFile.exists()) {
        storedFile.getParentFile().mkdirs();
        final DataFileWriter<GroupCheckpoint> dataFileWriter = new DataFileWriter<>(groupCheckpointDatumWriter);
        dataFileWriter.create(checkpoint.getSchema(), storedFile);
        dataFileWriter.append(checkpoint);
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
    // Delete all the unnecessary states within the stateMaps of stateful operators.
    for (final ExecutionDag ed : group.getExecutionDags().values()) {
      for (final ExecutionVertex ev : ed.getDag().getVertices()) {
        if (ev.getType() == ExecutionVertex.Type.OPERATOR) {
          final Operator op = ((DefaultPhysicalOperatorImpl) ev).getOperator();
          if (op instanceof StateHandler) {
            final StateHandler stateHandler = (StateHandler) op;
            stateHandler.removeOldStates(checkpoint.getMinimumLatestCheckpointTimestamp());
          }
        }
      }
    }

    return CheckpointResult.newBuilder()
        .setIsSuccess(true)
        .setMsg("Successfully checkpointed group " + tuple.getKey())
        .setPathToCheckpoint(getGroupCheckpointFile(groupId).toString())
        .build();
  }

  @Override
  public GroupCheckpoint loadSavedGroupState(final String groupId) throws IOException {
    // Load the file.
    final File storedFile = getGroupCheckpointFile(groupId);
    final DataFileReader<GroupCheckpoint> dataFileReader = new DataFileReader<>(storedFile, groupCheckpointDatumReader);
    GroupCheckpoint mgc = null;
    mgc = dataFileReader.next(mgc);
    if (mgc != null) {
      LOG.log(Level.INFO, "Checkpoint file found. groupId is " + groupId);
    } else {
      LOG.log(Level.WARNING, "Checkpoint file not found or error during loading. groupId is " + groupId);
    }
    return mgc;
  }

  @Override
  public List<AvroDag> loadSavedQueries(final List<String> queryIdList) throws IOException {
    final List<AvroDag> savedQueries = new ArrayList<>();
    for (final String queryId : queryIdList) {
      final File storedFile = getQueryStoreFile(queryId);
      final DataFileReader<AvroDag> dataFileReader = new DataFileReader<>(storedFile, avroDagDatumReader);
      AvroDag avroDag = null;
      avroDag = dataFileReader.next(avroDag);
      savedQueries.add(avroDag);
    }
    return savedQueries;
  }
}
