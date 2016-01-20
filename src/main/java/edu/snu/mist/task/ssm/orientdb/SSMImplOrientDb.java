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
import edu.snu.mist.task.ssm.SSM;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.Identifier;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This is the OrientDB implementation of the SSM, using a Document-store type of database.
 * The key (document) is the identifier of the operator.
 * There is only one field at all times - "state". All values (states) are stored under this field.
 */
public final class SSMImplOrientDb implements SSM {

    private static final Logger LOG = Logger.getLogger(SSMImplOrientDb.class.getName());
    private static ODatabaseDocumentTx db;
    //TODO where to close the database?

    @Inject
    private SSMImplOrientDb(@Parameter(OrientDbPath.class) final String orientDbPath,
                    @Parameter(OrientDbUser.class) final String orientDbUser,
                    @Parameter(OrientDbPassword.class) final String orientDbPassword,
                    @Parameter(OrientDbDropDB.class) final boolean orientDbDrop,
                    @Parameter(OrientDbSize.class) final int orientDbSize) {
        //open the database
        try {
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
                }
                else {
                    LOG.log(Level.INFO, "OrientDB: opening existing database.");
                    //Open with the id, password of the database.
                    db.open(orientDbUser, orientDbPassword);
                }
            }
            else {
                LOG.log(Level.INFO, "OrientDB: creating new database.");
                db.create();
            }
        } catch (Exception e){
            e.printStackTrace();
            LOG.log(Level.SEVERE, "OrientDB: failed to open.");
        }
    }

    @Override
    public <I> boolean set (final Identifier identifier, final I value){
       try {
           //If there is an existing identifier, replace the old value with the new value.
           for (ODocument doc: db.browseClass((identifier.toString()))){
                /*browseClass is an OrientDB method to browse for the key in the database.
                  Since this is a document db, it can have multiple keys (in our case the key is unique).
                  Thus, it returns an iterative class, so a for loop must be used.
                  But since we only have unique keys, this for loop will only be looped once.
                 */
                if (doc.containsField("state")){
                    doc.field("state", value);
                    LOG.log(Level.INFO, "OrientDB: saving (" + identifier+ ", "+ value+") to database.");
                    doc.save();
                    return true;
                }
            }

           //This is the case when the identifier was previously saved then deleted.
           final ODocument document = new ODocument(identifier.toString());
           document.field("state", value); //field() is an OrientDB method to set the value to a certain field.
           LOG.log(Level.INFO, "OrientDB: saving (" + identifier+ ", "+ value+") to database.");
           document.save(); //save() is an OrientDB method to save the document in the database.
           return true;

        } catch (Exception e) {
            try{
                //This is the case when the identifier was never saved.
                //TODO: This exception handling could lead to unnecessary exception handling load.

                final ODocument document = new ODocument(identifier.toString());
                document.field("state", value); //field() is an OrientDB method to set the value to a certain field.
                LOG.log(Level.INFO, "OrientDB: saving (" + identifier+ ", "+ value+") to database.");
                document.save(); //save() is an OrientDB method to save the document in the database.
                return true;
            } catch (Exception f){
                f.printStackTrace();
                LOG.log(Level.SEVERE, "OrientDB: failed to save.");
                return false;
            }
        }
    };

    @Override
    public <I> I get (final Identifier identifier){
        try{
            for (ODocument doc : db.browseClass(identifier.toString())){
                if (doc!=null){
                    return doc.field("state");
                }
                else{
                    LOG.log(Level.SEVERE, "OrientDB: failed to get state. Document is null.");
                    return null;
                }
            }
            LOG.log(Level.SEVERE, "OrientDB: failed to get state. The key does not exist in the database.");
            return null;

        } catch (Exception e){
            e.printStackTrace();
            LOG.log(Level.SEVERE, "OrientDB: failed to get state.");
            return null;
        }
    };

    @Override
    public boolean delete (final Identifier identifier){
        try{
            for (ODocument doc : db.browseClass(identifier.toString())) {
                LOG.log(Level.INFO, "OrientDB: deleting (" + identifier + ", " + doc.field("state") +
                        ") from database.");
                db.delete(doc);
                return true;
            }
            LOG.log(Level.INFO, "OrientDB: deletion failed. The key does not exist in the database.");
            return false;

        } catch (Exception e){
            LOG.log(Level.INFO, "OrientDB: deletion failed.");
            return false;
        }
    };

}
