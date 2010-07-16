/*
%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------
%%
%% J. Muellerleile
%%
*/

package raptor.store;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.log4j.Logger;

import raptor.store.handlers.ResultHandler;
import raptor.util.RaptorUtils;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Durability;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;

public class BdbStore {
    final private static Logger log =
            Logger.getLogger(BdbStore.class);

    private Database db;
    
    private static Environment env;
    
    public static void initEnvironment(String directory) throws Exception {
        RaptorUtils.ensureDirectory(directory);
    	log.info("Initializing BDB environment in " + directory);
    	
    	EnvironmentConfig envConfig = new EnvironmentConfig();
    	envConfig.setAllowCreate(true);
    	envConfig.setCacheSize(218435456);
    	envConfig.setTransactional(true);
    	envConfig.setDurability(Durability.COMMIT_WRITE_NO_SYNC);
    	
    	env = new Environment(new File(directory), envConfig);
    }
    
    public static void closeEnvironment()
    {
    	env.close();
    }

    public BdbStore(String filename) throws Exception {
        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setAllowCreate(true);
        dbConfig.setSortedDuplicates(false);
        dbConfig.setTransactional(true);
        db = env.openDatabase(null, filename, dbConfig);
    }

    public void close() throws Exception {
    	db.close();
    }

    public boolean put(String key, String val) throws Exception {
        return put(key.getBytes("UTF-8"), val.getBytes("UTF-8"));
    }

    public boolean put(byte[] key, byte[] val) throws Exception {
        return (db.put(null,
                       new DatabaseEntry(key),
                       new DatabaseEntry(val)) == OperationStatus.SUCCESS);
    }

    public String get(String key) throws Exception {
        byte[] v = get(key.getBytes("UTF-8"));
        if (v == null) return null;
        return new String(v, "UTF-8");
    }

    public byte[] get(byte[] key) throws Exception {
        DatabaseEntry dbKey = new DatabaseEntry(key);
        DatabaseEntry dbVal = new DatabaseEntry();
        if (db.get(null, dbKey, dbVal, LockMode.DEFAULT) ==
                OperationStatus.SUCCESS) {
            return dbVal.getData();
        } else {
            return null;
        }
    }

    public Map<byte[], byte[]> getRange(byte[] keyStart,
                                        byte[] keyEnd,
                                        boolean startInclusive,
                                        boolean endInclusive)
            throws Exception {
        return getRange(keyStart,
                keyEnd,
                startInclusive,
                endInclusive,
                null);
    }

    public Map<byte[], byte[]> getRange(byte[] keyStart,
                                        byte[] keyEnd,
                                        boolean startInclusive,
                                        boolean endInclusive,
                                        final ResultHandler resultHandler)
            throws Exception {
        Cursor cursor = null;
        DatabaseEntry dbKey = new DatabaseEntry(keyStart);
        String dbKeyEnd = new String(keyEnd, "UTF-8");
        Map<byte[], byte[]> results = new HashMap<byte[], byte[]>();
        try {
            cursor = db.openCursor(null, null);
            DatabaseEntry dbVal = new DatabaseEntry();
            OperationStatus retVal = cursor.getSearchKeyRange(dbKey, dbVal, LockMode.DEFAULT);
            long ct = 0;
            if (retVal == OperationStatus.SUCCESS &&
                    cursor.count() > 0) {
                while (retVal == OperationStatus.SUCCESS) {
                    byte[] k = dbKey.getData();
                    byte[] v = dbVal.getData();
                    String kcomp = new String(k, "UTF-8");
                    if ((kcomp.compareTo(dbKeyEnd) > 0 &&
                            endInclusive &&
                            !kcomp.startsWith(dbKeyEnd)) ||
                            (kcomp.compareTo(dbKeyEnd) >= 0 &&
                                    !endInclusive)) {
                        break;
                    }
                    if ((ct == 0 && startInclusive) || ct > 0) {
                        if (resultHandler != null) {
                            resultHandler.handleResult(k, v);
                        } else {
                            results.put(k, v);
                        }
                    }
                    ct++;
                    dbKey = new DatabaseEntry();
                    dbVal = new DatabaseEntry();
                    retVal = cursor.getNext(dbKey, dbVal, LockMode.DEFAULT);
                }
            }
        } catch (DatabaseException ex) {
            log.info("com.sleepycat.db.DatabaseException: " + ex.toString());
        } finally {
            if (cursor != null) cursor.close();
        }
        return results;
    }

    public int countRange(byte[] keyStart,
                          byte[] keyEnd,
                          boolean startInclusive,
                          boolean endInclusive)
            throws Exception {
        Cursor cursor = null;
        DatabaseEntry dbKey = new DatabaseEntry(keyStart);
        String dbKeyEnd = new String(keyEnd, "UTF-8");
        int ct = 0, rc = 0;
        try {
            cursor = db.openCursor(null, null);
            DatabaseEntry dbVal = new DatabaseEntry();
            OperationStatus retVal = cursor.getSearchKeyRange(dbKey, dbVal, LockMode.DEFAULT);
            if (retVal == OperationStatus.SUCCESS &&
                    cursor.count() > 0) {
                while (retVal == OperationStatus.SUCCESS) {
                    byte[] k = dbKey.getData();
                    String kcomp = new String(k, "UTF-8");
                    if ((kcomp.compareTo(dbKeyEnd) > 0 &&
                            endInclusive &&
                            !kcomp.startsWith(dbKeyEnd)) ||
                            (kcomp.compareTo(dbKeyEnd) >= 0 &&
                                    !endInclusive)) {
                        break;
                    }
                    if ((ct == 0 && startInclusive) || ct > 0) {
                        rc++;
                    }
                    ct++;
                    dbKey = new DatabaseEntry();
                    dbVal = new DatabaseEntry();
                    retVal = cursor.getNext(dbKey, dbVal, LockMode.DEFAULT);
                }
            }
        } finally {
            if (cursor != null) cursor.close();
        }
        return rc;
    }

    public List<byte[]> getRawKeyRange(byte[] keyStart,
                                       byte[] keyEnd,
                                       boolean startInclusive,
                                       boolean endInclusive) throws Exception {
        Cursor cursor = null;
        DatabaseEntry dbKey = new DatabaseEntry(keyStart);
        String dbKeyEnd = new String(keyEnd, "UTF-8");
        int ct = 0;
        List<byte[]> results = new ArrayList<byte[]>();
        try {
            cursor = db.openCursor(null, null);
            DatabaseEntry dbVal = new DatabaseEntry();
            OperationStatus retVal = cursor.getSearchKeyRange(dbKey, dbVal, LockMode.DEFAULT);
            if (retVal == OperationStatus.SUCCESS &&
                    cursor.count() > 0) {
                while (retVal == OperationStatus.SUCCESS) {
                    byte[] k = dbKey.getData();
                    String kcomp = new String(k, "UTF-8");
                    if ((kcomp.compareTo(dbKeyEnd) > 0 &&
                            endInclusive &&
                            !kcomp.startsWith(dbKeyEnd)) ||
                            (kcomp.compareTo(dbKeyEnd) >= 0 &&
                                    !endInclusive)) {
                        break;
                    }
                    if ((ct == 0 && startInclusive) || ct > 0) {
                        results.add(k);
                    }
                    ct++;
                    dbKey = new DatabaseEntry();
                    dbVal = new DatabaseEntry();
                    retVal = cursor.getNext(dbKey, dbVal, LockMode.DEFAULT);
                }
            }
        } finally {
            if (cursor != null) cursor.close();
        }
        return results;
    }


    public void sync() throws Exception {
        db.sync();
    }

    public boolean delete(byte[] key) throws Exception {
    	return (db.delete(null, new DatabaseEntry(key)) ==
                OperationStatus.SUCCESS);
    }

    public boolean exists(byte[] key) throws Exception {
    	// BDB-JE doesn't seem to have a simple existence query. Emulate one by
    	// using the partial retrieval functionality to avoid loading the data.
    	DatabaseEntry value = new DatabaseEntry();
    	value.setPartial(0, 0, true);
    	return (db.get(null, new DatabaseEntry(key), value, LockMode.READ_UNCOMMITTED) ==
                OperationStatus.SUCCESS);
    }
}
