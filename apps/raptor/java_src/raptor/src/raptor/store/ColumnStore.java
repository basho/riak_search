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

import com.sleepycat.bind.tuple.TupleInput;
import com.sleepycat.db.*;
import org.apache.log4j.Logger;
import raptor.store.column.ColumnKey;
import raptor.store.column.ColumnKeyTupleBinding;
import raptor.store.handlers.ResultHandler;
import raptor.util.ConsistentHash;
import raptor.util.RaptorUtils;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class ColumnStore implements Runnable {
    final private static Logger log =
            Logger.getLogger(ColumnStore.class);
    final private static int DEFAULT_PARTITIONS = 4;
    final private static String DB_PFX = "db";

    private ConcurrentHashMap<String, BtreeStore>
            stores = new ConcurrentHashMap<String, BtreeStore>();
    private ConcurrentHashMap<String, String>
            metadataCache = new ConcurrentHashMap<String, String>();
    private ConcurrentHashMap<byte[], byte[]>
            entryMetadataCache = new ConcurrentHashMap<byte[], byte[]>();
    private ConsistentHash<String> tableHash;
    private int partitions;
    private HashStore metadata;
    private HashStore entryMetadata;
    private Lock metadataLock = new ReentrantLock();
    private Lock entryMetadataLock = new ReentrantLock();
    private Environment env;
    private String directory;
    private String logDirectory;

    final public static String METADATA_COUNT = "metadata.count";
    final public static String __TABLES__ = "__tables__";
    final private static String __TABLE_DOCID_SEP__ = "`";

    public ColumnStore(String directory) throws Exception {
        this(directory, directory, DEFAULT_PARTITIONS);
    }

    public ColumnStore(String directory, String logDirectory, int partitions)
            throws Exception {
        open(directory, logDirectory, partitions);
    }

    private void open() throws Exception {
        open(this.directory, this.logDirectory, this.partitions);
    }

    private void open(String directory, String logDirectory, int partitions) throws Exception {
        this.directory = directory;
        this.logDirectory = logDirectory;
        this.partitions = partitions;
        log.info("ensureDirectory(" + directory + ")");
        RaptorUtils.ensureDirectory(directory);
        env = BtreeStore.getDefaultEnvironment(directory, logDirectory);
        tableHash = RaptorUtils.createConsistentHash(DB_PFX, partitions);
        for (int i = 0; i < partitions; i++) {
            log.info("Opening " + DB_PFX + i + ".column");
            BtreeStore store = new BtreeStore(
                    env,
                    DB_PFX + i + ".column", DB_PFX + i, 4096);
            stores.put(DB_PFX + i, store);
        }
        log.info("Opening metadata.hash");
        metadata = new HashStore(env, "metadata.hash", "metadata");
        log.info("Opening entry_metadata.hash");
        entryMetadata = new HashStore(env, "entry_metadata.hash", "entry_metadata");
    }

    public void run() {
        while (true) {
            try {
                Thread.sleep(10000);
                sync();
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
    }

    private byte[] getColumnKey(String table, String key)
            throws Exception {
        return getColumnKey(table, key.getBytes("UTF-8"));
    }

    private byte[] getColumnKey(String table, byte[] key)
            throws Exception {
        if ((new String(key, "UTF-8")).equals("")) {
            return (table).getBytes("UTF-8");
        }
        ColumnKey ck = new ColumnKey();
        ck.setTable(table);
        ck.setKey(key);
        ColumnKeyTupleBinding keyBinding = new ColumnKeyTupleBinding();
        DatabaseEntry dbKey = new DatabaseEntry();
        keyBinding.objectToEntry(ck, dbKey);
        return dbKey.getData();
    }

    private ColumnKey toColumnKey(byte[] key)
            throws Exception {
        ColumnKeyTupleBinding cktb = new ColumnKeyTupleBinding();
        return cktb.entryToObject(new TupleInput(key));
    }

    private byte[] parseColumnKey(byte[] key)
            throws Exception {
        ColumnKeyTupleBinding cktb = new ColumnKeyTupleBinding();
        ColumnKey ck = cktb.entryToObject(new TupleInput(key));
        return ck.getKey();
    }

    public boolean put(String table, String key, String val)
            throws Exception {
        return put(table, key.getBytes("UTF-8"), val.getBytes("UTF-8"));
    }

    public boolean put(String table, byte[] key, byte[] val)
            throws Exception {
        boolean updateCount;
        byte[] columnKey = getColumnKey(table, key);
        BtreeStore store = stores.get(tableHash.get(table));
        updateCount = !store.exists(columnKey);
        if (store.put(columnKey, val)) {
            /*
            if (updateCount) incrementTableCount(table);
            // ensure system __tables__ entry
            if (!table.equals(__TABLES__)) {
                if (put(__TABLES__, table, ""+System.currentTimeMillis())) {
                    return true;
                } else {
                    return false;
                }
            } else return true;
            */
            return true;
        }
        return false;
    }

    public String get(String table, String key)
            throws Exception {
        BtreeStore store = stores.get(tableHash.get(table));
        byte[] v = store.get(getColumnKey(table, key));
        if (v == null) return null;
        return new String(v, "UTF-8");
    }

    public byte[] get(String table, byte[] key)
            throws Exception {
        BtreeStore store = stores.get(tableHash.get(table));
        return store.get(getColumnKey(table, key));
    }

    public boolean delete(String table, String key)
            throws Exception {
        return delete(table, key.getBytes("UTF-8"));
    }

    public boolean delete(String table, byte[] key)
            throws Exception {
        byte[] columnKey = getColumnKey(table, key);
        BtreeStore store = stores.get(tableHash.get(table));
        if (store.delete(columnKey)) {
            //decrementTableCount(table);
            return true;
        }
        return false;
    }

    protected boolean rawDelete(byte[] key)
            throws Exception {
        for (String k : stores.keySet()) {
            BtreeStore store = stores.get(k);
            boolean rval = store.delete(key);
            log.info("rawDelete: [" +
                    new String(key, "UTF-8") + "] " +
                    rval);
        }
        return true;
    }

    protected void reportWriteQueueSizes() throws Exception {
        for (String k : stores.keySet()) {
            BtreeStore store = stores.get(k);
            if (store.writeQueue.size() > 0) {
                log.info("[" + k + "] writeQueue.size() = " + store.writeQueue.size());
            }
        }
    }

    public boolean exists(String table, String key)
            throws Exception {
        return exists(table, key.getBytes("UTF-8"));
    }

    public boolean tableExists(String table) throws Exception {
        return null != getMetadata(table, METADATA_COUNT);
    }

    public boolean exists(String table, byte[] key)
            throws Exception {
        BtreeStore store = stores.get(tableHash.get(table));
        return store.exists(getColumnKey(table, key));
    }

    public Map<byte[], byte[]> getRange(String table,
                                        byte[] keyStart,
                                        byte[] keyEnd,
                                        boolean startInclusive,
                                        boolean endInclusive)
            throws Exception {
        return getRange(table,
                keyStart,
                keyEnd,
                startInclusive,
                endInclusive,
                false,
                null);
    }

    public Map<byte[], byte[]> getRange(String table,
                                        byte[] keyStart,
                                        byte[] keyEnd,
                                        boolean startInclusive,
                                        boolean endInclusive,
                                        ResultHandler resultHandler)
            throws Exception {
        return getRange(table,
                keyStart,
                keyEnd,
                startInclusive,
                endInclusive,
                false,
                resultHandler);
    }

    public Map<byte[], byte[]> getRange(String table,
                                        byte[] keyStart,
                                        byte[] keyEnd,
                                        boolean startInclusive,
                                        boolean endInclusive,
                                        boolean rawKeys,
                                        final ResultHandler resultHandler)
            throws Exception {
        BtreeStore store = stores.get(tableHash.get(table));
        byte[] ckStart = getColumnKey(table, keyStart);
        byte[] ckEnd = getColumnKey(table, keyEnd);
        if (resultHandler != null) {
            store.getRange(ckStart,
                    ckEnd,
                    startInclusive,
                    endInclusive,
                    new ResultHandler() {
                        public void handleResult(byte[] k, byte[] v) {
                            try {
                                resultHandler.handleResult(parseColumnKey(k), v);
                            } catch (Exception ex) {
                                ex.printStackTrace();
                            }
                        }
                    });
            return null;
        } else {
            Map<byte[], byte[]> results =
                    store.getRange(ckStart,
                            ckEnd,
                            startInclusive,
                            endInclusive,
                            null);
            Map<byte[], byte[]> r = new HashMap<byte[], byte[]>();
            for (byte[] k : results.keySet()) {
                byte[] v = results.get(k);
                if (rawKeys) {
                    r.put(k, v);
                } else {
                    r.put(parseColumnKey(k), v);
                }
            }
            return r;
        }
    }

    public int countRange(String table,
                          String keyStart,
                          String keyEnd,
                          boolean startInclusive,
                          boolean endInclusive) throws Exception {
        BtreeStore store = stores.get(tableHash.get(table));
        byte[] ckStart = getColumnKey(table, keyStart);
        byte[] ckEnd = getColumnKey(table, keyEnd);
        return store.countRange(ckStart,
                ckEnd,
                startInclusive,
                endInclusive);
    }

    public List<byte[]> getRawKeys(String table,
                                   String keyStart,
                                   String keyEnd,
                                   boolean startInclusive,
                                   boolean endInclusive) throws Exception {
        BtreeStore store = stores.get(tableHash.get(table));
        byte[] ckStart = getColumnKey(table, keyStart);
        byte[] ckEnd = getColumnKey(table, keyEnd);
        return store.getRawKeyRange(ckStart,
                ckEnd,
                startInclusive,
                endInclusive);
    }

    public Map<String, String> getRange(String table,
                                        String keyStart,
                                        String keyEnd,
                                        boolean startInclusive,
                                        boolean endInclusive) throws Exception {
        BtreeStore store = stores.get(tableHash.get(table));
        byte[] ckStart = keyStart.getBytes("UTF-8");
        byte[] ckEnd = keyEnd.getBytes("UTF-8");
        Map<byte[], byte[]> results =
                getRange(table, ckStart, ckEnd, startInclusive, endInclusive);
        Map<String, String> r = new TreeMap<String, String>();
        for (byte[] k : results.keySet()) {
            String kStr = new String(k, "UTF-8");
            String kVal;
            if (results.get(k) != null) {
                kVal = new String(results.get(k), "UTF-8");
            } else {
                kVal = "";
            }
            r.put(kStr, kVal);
        }
        return r;
    }

    public Map<String, String> getAll(String table) throws Exception {
        return getRange(table, "", "", true, true);
    }

    public List<byte[]> getRangeKeys(String table,
                                     byte[] keyStart,
                                     byte[] keyEnd,
                                     boolean startInclusive,
                                     boolean endInclusive) throws Exception {
        List<byte[]> keys = new ArrayList<byte[]>();
        Set<String> keyhs = new HashSet<String>();
        Map<byte[], byte[]> results =
                getRange(table, keyStart, keyEnd, startInclusive, endInclusive);
        for (byte[] k : results.keySet()) {
            String kStr = new String(k, "UTF-8");
            if (!keyhs.contains(kStr)) {
                keys.add(k);
                keyhs.add(kStr);
            }
        }
        return keys;
    }

    public List<String> getRangeKeys(String table,
                                     String keyStart,
                                     String keyEnd,
                                     boolean startInclusive,
                                     boolean endInclusive) throws Exception {
        List<String> keys = new ArrayList<String>();
        Set<String> keyhs = new HashSet<String>();
        List<byte[]> r = getRangeKeys(table,
                keyStart.getBytes("UTF-8"),
                keyEnd.getBytes("UTF-8"),
                startInclusive,
                endInclusive);
        for (byte[] k : r) {
            String kStr = new String(k, "UTF-8");
            if (!keyhs.contains(kStr)) {
                keys.add(kStr);
                keyhs.add(kStr);
            }
        }
        Collections.sort(keys);
        return keys;
    }

    public List<String> getKeys(String table) throws Exception {
        return getRangeKeys(table, "", "", true, true);
    }

    public List<String> getTables() throws Exception {
        return getKeys(__TABLES__);
    }

    public void sync() throws Exception {
        for (BtreeStore store : stores.values()) {
            store.sync();
        }
        metadata.sync();
        entryMetadata.sync();
    }

    public void checkpoint() throws Exception {
        CheckpointConfig config = new CheckpointConfig();
        config.setKBytes(1000);
        config.setForce(true);
        env.checkpoint(config);
    }

    public void close() throws Exception {
        //metadataLock.lock();
        try {
            for (BtreeStore store : stores.values()) {
                store.close();
            }
            metadata.close();
            entryMetadata.close();
            env.close();
        } finally {
            //metadataLock.unlock();
        }
    }

    public long count(String table) throws Exception {
        String val = getMetadata(table, METADATA_COUNT);
        if (val == null) return 0;
        return Long.parseLong(val);
    }

    public String getMetadata(String table, String label)
            throws Exception {
        //metadataLock.lock();
        try {
            String key = makeMetadataKey(table, label);
            if (null == metadataCache.get(key)) {
                String value = metadata.get(key);
                if (value == null) return null;
                metadataCache.put(key, value);
                return value;
            }
            return metadataCache.get(key);
        } finally {
            //metadataLock.unlock();
        }
    }

    public boolean setMetadata(String table, String label, String value)
            throws Exception {
        //metadataLock.lock();
        try {
            String key = makeMetadataKey(table, label);
            if (metadata.put(key, value)) {
                metadataCache.put(key, value);
                return true;
            }
            return false;
        } finally {
            //metadataLock.unlock();
        }
    }

    private String makeMetadataKey(String table, String label)
            throws Exception {
        return new String(getColumnKey(table, label), "UTF-8");
    }

    private void incrementTableCount(String table)
            throws Exception {
        incrementTableCount(table, 1);
    }

    private void incrementTableCount(String table, long delta)
            throws Exception {
        //metadataLock.lock();
        try {
            String val = getMetadata(table, METADATA_COUNT);
            if (val == null) {
                metadataLock.lock();
                try {
                    if (delta > 0) {
                        setMetadata(table, METADATA_COUNT, "1");
                    } else {
                        setMetadata(table, METADATA_COUNT, "0");
                    }
                } finally {
                    metadataLock.unlock();
                }
            } else {
                long count = Long.parseLong(val);
                setMetadata(table, METADATA_COUNT, "" + (count + delta));
            }
        } finally {
            //metadataLock.unlock();
        }
    }

    private void decrementTableCount(String table)
            throws Exception {
        incrementTableCount(table, -1);
    }

    public byte[] getTermEntryMetadata(String table,
                                       byte[] docId,
                                       String term,
                                       String label) throws Exception {
        String compositeTable = makeTermEntryMetadataKey(table, term);
        return getEntryMetadata(compositeTable, docId, label);
    }

    public byte[] getEntryMetadata(String table,
                                   byte[] docId,
                                   String label) throws Exception {
        //entryMetadataLock.lock();
        try {
            byte[] key = makeEntryMetadataKey(table, docId, label);
            if (null == entryMetadataCache.get(key)) {
                byte[] value = entryMetadata.get(key);
                if (value == null) return null;
                return value;
            }
            return entryMetadataCache.get(key);
        } finally {
            //entryMetadataLock.unlock();
        }
    }

    // set entry metadata for a specific docId + term
    public boolean setTermEntryMetadata(String table,
                                        byte[] docId,
                                        String term,
                                        String label,
                                        byte[] value) throws Exception {
        String compositeTable = makeTermEntryMetadataKey(table, term);
        return setEntryMetadata(compositeTable, docId, label, value);
    }

    // set entry metadata for a specific docId
    public boolean setEntryMetadata(String table,
                                    byte[] docId,
                                    String label,
                                    byte[] value) throws Exception {
        //entryMetadataLock.lock();
        try {
            byte[] key = makeEntryMetadataKey(table, docId, label);
            if (entryMetadata.put(key, value)) {
                return true;
            }
            return false;
        } finally {
            //entryMetadataLock.unlock();
        }
    }

    // delete entry metadata for a specific docId + term
    public boolean deleteTermEntryMetadata(String table,
                                           byte[] docId,
                                           String term,
                                           String label) throws Exception {
        String compositeTable = makeTermEntryMetadataKey(table, term);
        return deleteEntryMetadata(compositeTable, docId, label);
    }

    public boolean deleteEntryMetadata(String table,
                                       byte[] docId,
                                       String label) throws Exception {
        //entryMetadataLock.lock();
        try {
            byte[] key = makeEntryMetadataKey(table, docId, label);
            if (entryMetadata.delete(key)) {
                entryMetadataCache.remove(key);
                return true;
            }
            return false;
        } finally {
            //entryMetadataLock.unlock();
        }
    }

    private byte[] makeEntryMetadataKey(String table, byte[] docId, String label)
            throws Exception {
        String key1 = table +
                __TABLE_DOCID_SEP__ +
                new String(docId, "UTF-8");
        return getColumnKey(key1, label);
    }

    private String makeTermEntryMetadataKey(String table,
                                            String term) throws Exception {
        return table +
                __TABLE_DOCID_SEP__ +
                term;
    }

    public void incrementSequence(String table)
            throws Exception {
        incrementSequence(table, 1);
    }

    public void incrementSequence(String table, int delta)
            throws Exception {
        Sequence seq = getSequence(table);
        seq.get(null, delta);
    }

    private Sequence getSequence(String table)
            throws Exception {
        BtreeStore store = stores.get(tableHash.get(table));
        return getSequence(store, table);
    }

    private Sequence getSequence(BtreeStore store, String table)
            throws Exception {
        SequenceConfig sequenceConfig = new SequenceConfig();
        sequenceConfig.setAllowCreate(true);
        sequenceConfig.setAutoCommitNoSync(true);
        sequenceConfig.setRange(0, Long.MAX_VALUE);
        sequenceConfig.setWrap(false);
        sequenceConfig.setInitialValue(0);
        sequenceConfig.setDecrement(false);
        Database db = store.getDatabase();
        DatabaseEntry tableSeqKey = new DatabaseEntry(table.getBytes("UTF-8"));
        return db.openSequence(null, tableSeqKey, sequenceConfig);
    }

    public long getSequenceValue(String table)
            throws Exception {
        BtreeStore store = stores.get(tableHash.get(table));
        return getSequenceValue(store, table);
    }

    private long getSequenceValue(BtreeStore store, String table)
            throws Exception {
        Sequence sequence = getSequence(store, table);
        StatsConfig statsConfig = new StatsConfig();
        statsConfig.setClear(false);
        statsConfig.setFast(true);
        SequenceStats stats = sequence.getStats(statsConfig);
        long count = stats.getCurrent();
        sequence.close();
        return count;
    }

    public static void main(String args[]) throws Exception {
        ColumnStore store = new ColumnStore("test.column2", "bdb-log2", 8);
        log.info("tableExists(xyzzy): " + store.tableExists("xyzzy"));
        store.put("fruits", "apple", "fruit: apple");
        store.put("fruits", "banana", "fruit: banana");
        store.put("fruits", "carrot", "vegetable: carrot");
        store.put("fruits", "potato", "vegetable: potato");
        store.put("fruits", "pineapple", "fruit: pineapple");
        log.info("tableExists(fruits): " + store.tableExists("fruits"));
        log.info("getRange(fruits, a, p, true, true) = ");
        Map<String, String> r = store.getRange("fruits", "a", "p", true, true);
        for (String k : r.keySet()) {
            log.info("getRange: " + k + " -> " + r.get(k));
        }
        log.info("getRangeKeys(fruits, a, p, true, true) = " +
                store.getRangeKeys("fruits", "a", "p", true, true));
        log.info("getKeys(fruits) = " +
                store.getKeys("fruits"));
        log.info("");

        log.info("store.get(fruits, potato): " + store.get("fruits", "potato"));
        log.info("store.exists(fruits, potato): " + store.exists("fruits", "potato"));
        log.info("store.exists(fruits, carrot): " + store.exists("fruits", "carrot"));
        log.info("store.exists(fruits, pineapple): " + store.exists("fruits", "pineapple"));
        log.info("store.delete(fruits, banana): " + store.delete("fruits", "banana"));
        log.info("store.exists(fruits, banana): " + store.exists("fruits", "banana"));
        log.info("store.count(fruits): " + store.count("fruits"));

        store.close();
    }
}

