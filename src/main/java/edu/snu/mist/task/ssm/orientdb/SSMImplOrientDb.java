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

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.record.impl.ODocument;
import edu.snu.mist.task.ssm.SSM;
import org.apache.reef.wake.Identifier;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This is the OrientDB implementation of the SSM, using a Document-store type of database.
 * The key (document) is the identifier of the operator.
 * There is only one field at all times - "state". All values (states) are stored under this field.
 */
public final class SSMImplOrientDb implements SSM {

    private static final Logger LOG = Logger.getLogger(SSMImplOrientDb.class.getName());
    public static ODatabaseDocumentTx db;

    @Override
    public boolean open (){
        try {
            //The database is opened under this url.
            db = new ODatabaseDocumentTx("plocal:/tmp/SSM-orientdb");
            if (db.exists()){
                LOG.log(Level.INFO, "Opening existing OrientDB");
                //The id, password of the database.
                db.open("admin", "admin");
                db.drop();
                db.create();
            }
            else {
                LOG.log(Level.INFO, "Creating new OrientDB");
                db.create();
            }
            return true;
        } catch (Exception e){
            e.printStackTrace();
            LOG.log(Level.SEVERE, "OrientDB failed to open.");
            return false;
        }
    }

    @Override
    public boolean close(){
        try {
            db.close();
            return true;
        } catch (Exception e){
            e.printStackTrace();
            LOG.log(Level.SEVERE, "OrientDB failed to close.");
            return false;
        }
    }

    @Override
    public <I> boolean set (final Identifier identifier, final I value){
        try {
            for (ODocument doc: db.browseClass((identifier.toString()))){
                //If there is an existing identifier, update the document
                if (doc.containsField("state")){
                    doc.field("state", value);
                    LOG.log(Level.INFO, "saving (" + identifier+ ", "+ value+")");
                    doc.save();
                    return true;
                }
            }
            return false;

        } catch (IllegalArgumentException e) {
            try{
                //If there is no existing identifier, create a new document
                //TODO: This exception handling could lead to unnecessary exception handling load.
                final ODocument document = new ODocument(identifier.toString());
                document.field("state", value);
                LOG.log(Level.INFO, "saving (" + identifier+ ", "+ value+") to OrientDB");

                document.save();
                return true;
            } catch (IllegalArgumentException f){
                f.printStackTrace();
                LOG.log(Level.SEVERE, "OrientDB failed to save.");
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
                    LOG.log(Level.SEVERE, "OrientDB failed to get state.");
                    return null;
                }
            }
            return null;

        } catch (IllegalArgumentException e){
            e.printStackTrace();
            LOG.log(Level.SEVERE, "OrientDB failed to get state.");
            return null;
        }
    };

    @Override
    public boolean delete (final Identifier identifier){
        try{
            for (ODocument doc : db.browseClass(identifier.toString())) {
                LOG.log(Level.INFO, "deleting (" + identifier + ", " + doc.field("state") + ")");
                doc.delete();
            }
            return true;

        } catch (IllegalArgumentException e){
            LOG.log(Level.SEVERE, "The key does not exist in OrientDB");
            return false;
        }
    };

}
