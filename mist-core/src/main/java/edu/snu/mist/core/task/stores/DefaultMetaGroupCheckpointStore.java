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
import edu.snu.mist.formats.avro.CheckpointResult;
import edu.snu.mist.formats.avro.MetaGroupCheckpoint;
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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Level;
import java.util.logging.Logger;

public final class DefaultMetaGroupCheckpointStore implements MetaGroupCheckpointStore {

  private static final Logger LOG = Logger.getLogger(DefaultMetaGroupCheckpointStore.class.getName());

  private final String tmpFolderPath;

  /**
   * A writer that stores MetaGroupCheckpoints.
   */
  private final DatumWriter<MetaGroupCheckpoint> datumWriter;

  /**
   * A reader that reads stored MetaGroupCheckpoints.
   */
  private final DatumReader<MetaGroupCheckpoint> datumReader;

  /**
   * Contains a groupId as the key
   * and the latch that indicates whether checkpointing for this groupId has finished as a value.
   */
  private Map<String, CountDownLatch> groupIdCheckpointingLatchMap;

  /**
   * Contains a groupId as the key
   * and the latch that indicates whether loading for this groupId has finished as a value.
   */
  private Map<String, CountDownLatch> groupIdLoadingLatchMap;

  @Inject
  private DefaultMetaGroupCheckpointStore(@Parameter(TempFolderPath.class) final String tmpFolderpath) {
    this.tmpFolderPath = tmpFolderpath;
    this.datumWriter = new SpecificDatumWriter<>(MetaGroupCheckpoint.class);
    this.datumReader = new SpecificDatumReader<>(MetaGroupCheckpoint.class);
    this.groupIdCheckpointingLatchMap = new ConcurrentHashMap<>();
    this.groupIdLoadingLatchMap = new ConcurrentHashMap<>();
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

  private File getMetaGroupCheckpointFile(final String groupId) {
    final StringBuilder sb = new StringBuilder(groupId);
    sb.append(".groupcp");
    return new File(tmpFolderPath, sb.toString());
  }


  @Override
  public CheckpointResult saveMetaGroupCheckpoint(final Tuple<String, MetaGroupCheckpoint> tuple) {
    try {
      final String groupId = tuple.getKey();
      final MetaGroupCheckpoint gmc = tuple.getValue();
      if (groupIdCheckpointingLatchMap.putIfAbsent(groupId, new CountDownLatch(1)) != null) {
        // If checkpointing is waiting to be done, skip this checkpoint.
        LOG.log(Level.INFO, "This group " + groupId + " is already waiting to be checkpointed.");
        return CheckpointResult.newBuilder()
            .setIsSuccess(false)
            .setMsg(tuple.getKey() + " is already waiting to be checkpointed")
            .setPathToCheckpoint("")
            .build();
      } else {
        // If loading is waiting to be done on this groupId, wait until that loading finishes before checkpointing.
        CountDownLatch loadingLatch = groupIdLoadingLatchMap.get(groupId);
        while (loadingLatch != null && loadingLatch.getCount() > 0) {
          loadingLatch.await();
          loadingLatch = groupIdLoadingLatchMap.get(groupId);
        }

        // Write the file.
        final File storedFile = getMetaGroupCheckpointFile(groupId);
        if (storedFile.exists()) {
          storedFile.delete();
          LOG.log(Level.INFO, "Checkpoint deleted for groupId: {0}", groupId);
        }
        if (!storedFile.exists()) {
          storedFile.getParentFile().mkdirs();
          final DataFileWriter<MetaGroupCheckpoint> dataFileWriter = new DataFileWriter<>(datumWriter);
          dataFileWriter.create(gmc.getSchema(), storedFile);
          dataFileWriter.append(gmc);
          dataFileWriter.close();
          LOG.log(Level.INFO, "Checkpoint completed for groupId: {0}", groupId);
        }

        groupIdCheckpointingLatchMap.get(groupId).countDown();
        groupIdCheckpointingLatchMap.remove(groupId);
        LOG.log(Level.INFO, "Checkpoint submitted for groupId: {0}", tuple.getKey());
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
  public MetaGroupCheckpoint loadMetaGroupCheckpoint(final String groupId) throws IOException {
    try {
      if (groupIdLoadingLatchMap.putIfAbsent(groupId, new CountDownLatch(1)) != null) {
        // If loading is waiting to be done, skip this loading.
        LOG.log(Level.INFO, "This group " + groupId + " is already waiting to be loaded.");
      } else {
        // If checkpointing is waiting to be done on this groupId, wait until that checkpoint finishes before loading.
        CountDownLatch checkpointingLatch = groupIdCheckpointingLatchMap.get(groupId);
        while (checkpointingLatch != null && checkpointingLatch.getCount() > 0) {
          checkpointingLatch.await();
          checkpointingLatch = groupIdCheckpointingLatchMap.get(groupId);
        }
      }
    } catch (final InterruptedException e) {
        e.printStackTrace();
    }

    // Write the file.
    final File storedFile = getMetaGroupCheckpointFile(groupId);
    final DataFileReader<MetaGroupCheckpoint> dataFileReader =
        new DataFileReader<MetaGroupCheckpoint>(storedFile, datumReader);
    MetaGroupCheckpoint mgc = null;
    mgc = dataFileReader.next(mgc);
    if (mgc != null) {
      LOG.log(Level.INFO, "Checkpoint file found. groupId is " + groupId);
    } else {
      LOG.log(Level.WARNING, "Checkpoint file not found or error during loading. groupId is " + groupId);
    }

    groupIdLoadingLatchMap.get(groupId).countDown();
    groupIdLoadingLatchMap.remove(groupId);
    return mgc;
  }
}
