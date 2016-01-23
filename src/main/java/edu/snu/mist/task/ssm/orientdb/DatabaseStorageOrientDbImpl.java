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

package edu.snu.mist.task.ssm.orientdb;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.record.impl.ODocument;
import edu.snu.mist.task.ssm.*;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.Identifier;
import com.orientechnologies.orient.core.serialization.OBase64Utils;

import javax.inject.Inject;
import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This is the OrientDB implementation of the DatabaseStorage of the SSM, using a Document-store type of database.
 * The key (document) is the identifier of the operator.
 * There is only one field at all times - "operatorStateMap". All values (states) are stored under this field.
 */
public final class DatabaseStorageOrientDbImpl implements DatabaseStorage, Serializable {
  private static final Logger LOG = Logger.getLogger(DatabaseStorageOrientDbImpl.class.getName());
  private static ODatabaseDocumentTx db;

  @Inject
  private DatabaseStorageOrientDbImpl(@Parameter(OrientDbPath.class) final String orientDbPath,
                          @Parameter(OrientDbUser.class) final String orientDbUser,
                          @Parameter(OrientDbPassword.class) final String orientDbPassword,
                          @Parameter(OrientDbDropDb.class) final boolean orientDbDrop,
                          @Parameter(OrientDbSize.class) final int orientDbSize) throws DatabaseOpenException {
    //open the database
    //The database is opened under this url.
    db = new ODatabaseDocumentTx(orientDbPath);
    OGlobalConfiguration.DISK_CACHE_SIZE.setValue(orientDbSize); // this is in megabytes
    if (db.exists()){ //OrientDB method to check whether the db exists in the path.
      if(orientDbDrop){
        //If drop is true, drop and create the database.
        LOG.log(Level.INFO, "OrientDB: dropping and creating a new database.");
        db.open(orientDbUser, orientDbPassword);
        db.drop();
        db.create();
      } else {
        LOG.log(Level.INFO, "OrientDB: opening existing database.");
        //Open with the id, password of the database.
        db.open(orientDbUser, orientDbPassword);
      }
    } else {
      LOG.log(Level.INFO, "OrientDB: creating new database.");
      db.create();
    }
  }

  @Override
  public HashMap<Identifier, OperatorState> read(final Identifier queryId) throws DatabaseReadException {
    for (final ODocument doc : db.browseClass(queryId.toString())){
      if (doc!=null){
        return doc.field("operatorStateMap");
      } else{
        LOG.log(Level.WARNING, "OrientDB: failed to get state. Document is null.");
        return null;
      }
    }
    LOG.log(Level.WARNING, "OrientDB: failed to get state. The key does not exist in the database.");
    return null;
    //TODO [MIST-108]: Return something other than null.
  };

  @Override
  public boolean update(final Identifier queryId, final HashMap<Identifier, OperatorState> operatorStateMap)
      throws DatabaseUpdateException, IOException {
    try {
      //If there is an existing identifier, replace the old value with the new value.
      for (final ODocument doc: db.browseClass(queryId.toString())){
        /*browseClass is an OrientDB method to browse for the key in the database.
                  Since this is a document db, it can have multiple keys (in our case the key is unique).
                  Thus, it returns an iterative class, so a for loop must be used.
                  But since we only have unique keys, this for loop will only be looped once.
                 */
        if (doc.containsField("operatorStateMap")){
          doc.field("operatorStateMap", OBase64Utils.encodeObject(operatorStateMap));
          LOG.log(Level.INFO, "OrientDB: saving " + queryId+ " and its states to database.");
          doc.save();
          return true;
        }
      }

      //This is the case when the identifier was previously saved then deleted.
      final ODocument document = new ODocument(queryId.toString());
      document.field("operatorStateMap", OBase64Utils.encodeObject(operatorStateMap));
      //field() is an OrientDB method to set the value to a certain field.
      LOG.log(Level.INFO, "OrientDB: saving " + queryId+ " and its states to database.");
      document.save(); //save() is an OrientDB method to save the document in the database.
      return true;

    } catch (final IllegalArgumentException e) {
      //This is the case when the identifier was never saved.
      //TODO[MIST-100]: This exception handling could lead to unnecessary exception handling load.

      final ODocument document = new ODocument(queryId.toString());
      document.field("operatorStateMap", OBase64Utils.encodeObject(operatorStateMap));
      LOG.log(Level.INFO, "OrientDB: saving " + queryId+ " and its states to database.");
      document.save();
      return true;
    }
  };

  @Override
  public boolean delete(final Identifier queryId) throws DatabaseDeleteException {
    for (final ODocument doc : db.browseClass(queryId.toString())) {
      LOG.log(Level.INFO, "OrientDB: deleting " + queryId + " from database.");
      db.delete(doc);
      return true;
    }
    LOG.log(Level.INFO, "OrientDB: deletion failed. The key does not exist in the database.");
    return false;
  };
}