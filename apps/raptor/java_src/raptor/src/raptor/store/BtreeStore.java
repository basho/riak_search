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

import raptor.store.bdb.DefaultBDBMessageHandler;
import raptor.store.handlers.ResultHandler;
import raptor.util.RaptorUtils;

import com.sleepycat.db.Cursor;
import com.sleepycat.db.Database;
import com.sleepycat.db.DatabaseConfig;
import com.sleepycat.db.DatabaseEntry;
import com.sleepycat.db.DatabaseType;
import com.sleepycat.db.Environment;
import com.sleepycat.db.EnvironmentConfig;
import com.sleepycat.db.LockMode;
import com.sleepycat.db.OperationStatus;

public class BtreeStore {
    final private static Logger log =
            Logger.getLogger(BtreeStore.class);

    private final DatabaseConfig databaseConfig;
    private Database db;
    private final Environment env;

    private static final DefaultBDBMessageHandler
            defaultMessageHandler = new DefaultBDBMessageHandler();

    public BtreeStore(String filename,
                      String name) throws Exception {
        this(getDefaultEnvironment(".", "."), filename, name);
    }

    public BtreeStore(String filename,
                      String directory,
                      String name) throws Exception {
        this(getDefaultEnvironment(directory, directory), filename, name);
    }

    public BtreeStore(String filename,
                      String directory,
                      String logDirectory,
                      String name) throws Exception {
        this(getDefaultEnvironment(directory, logDirectory), filename, name);
    }

    public BtreeStore(Environment env,
                      String filename,
                      String name) throws Exception {
        this.env = env;

        databaseConfig = new DatabaseConfig();
        databaseConfig.setAllowCreate(true);
        databaseConfig.setErrorStream(System.err);
        databaseConfig.setErrorHandler(defaultMessageHandler);
        databaseConfig.setFeedbackHandler(defaultMessageHandler);
        databaseConfig.setMessageHandler(defaultMessageHandler);
        databaseConfig.setErrorPrefix("<" + filename + ": " + name + "> ");
        databaseConfig.setType(DatabaseType.BTREE);
        databaseConfig.setReverseSplitOff(true);
        databaseConfig.setChecksum(true);
        databaseConfig.setSortedDuplicates(false);
        databaseConfig.setTransactional(true);
        databaseConfig.setReadUncommitted(true);
        db = this.env.openDatabase(null, filename, name, databaseConfig);
    }

    protected static Environment getDefaultEnvironment(String directory,
                                                       String logDirectory) {
        EnvironmentConfig envConf = new EnvironmentConfig();
        envConf.setAllowCreate(true);
        envConf.setInitializeLogging(true);
        envConf.setRunRecovery(true);
        envConf.setLogDirectory(new File(logDirectory));
        envConf.setCacheSize(218435456);
        envConf.setInitializeCache(true);
        envConf.setMMapSize(100000000);
        envConf.setMaxLogFileSize(64 * 1048576); // 64 MB
        envConf.setLogAutoRemove(true);
        envConf.setMessageHandler(defaultMessageHandler);
        envConf.setMessageStream(System.err);
        envConf.setPrivate(true);
        envConf.setTxnWriteNoSync(true);
        envConf.setTransactional(true);
        try {
            log.info("ensureDirectory(" + directory + ")");
            RaptorUtils.ensureDirectory(directory);
            log.info("ensureDirectory(" + directory + "/" + logDirectory + ")");
            RaptorUtils.ensureDirectory(directory + "/" + logDirectory);
            Environment env = new Environment(new File(directory), envConf);
            return env;
        } catch (Exception ex) {
            log.error("error getting default environment", ex);
            ex.printStackTrace();
        }
        return null;
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
        } catch (com.sleepycat.db.DatabaseException ex) {
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
    	return (db.exists(null, new DatabaseEntry(key)) ==
                OperationStatus.SUCCESS);
    }

    protected Database getDatabase() {
        return db;
    }

    protected Environment getEnvironment() {
        return env;
    }

    public static void main(String args[]) throws Exception {
        BtreeStore store = new BtreeStore("test.btree", "test");
        store.put("apple", "fruit: apple");
        store.put("banana", "fruit: banana");
        store.put("carrot", "vegetable: carrot");
        store.put("potato", "vegetable: potato");
        store.put("pineapple", "fruit: pineapple");

        Random r = new Random();
        long startTime = System.currentTimeMillis();
        for (int i = 0; i < 1000000; i++) {
            String k = "k" + i;
            String v = "v" + r.nextInt(1929398);
            store.put(k, v);
        }
        log.info("100k writes in " + ((System.currentTimeMillis() - startTime) / 1000) + " sec");
        log.info("starting reads...");
        startTime = System.currentTimeMillis();
        for (int i = 0; i < 100000; i++) {
            String k = "k" + i;
            store.get(k);
        }
        log.info("100k reads in " + ((System.currentTimeMillis() - startTime) / 1000) + " sec");

        log.info("store.get(potato): " + store.get("potato"));
        store.close();
    }
}
