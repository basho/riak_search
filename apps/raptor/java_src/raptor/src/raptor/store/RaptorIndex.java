// -------------------------------------------------------------------
//
// Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
//
// -------------------------------------------------------------------

package raptor.store;

import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap.Builder;
import org.apache.log4j.Logger;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.json.JSONArray;
import org.json.JSONObject;
import raptor.server.RaptorServer;
import raptor.store.handlers.ResultHandler;

import java.util.*;

public class RaptorIndex {
    final private static Logger log =
            Logger.getLogger(RaptorIndex.class);
    final private static String TABLE_SEP = "/";
    final private static String IFT_SEP = ".";
    final private static String CATALOG_TABLE = "__sys._catalog_";
    final private static int STORE_COMMIT_INTERVAL = 60000; /* ms; todo: configurable? */
    final private static int LUCENE_COMMIT_INTERVAL = 5000; /* ms; todo: configurable?  */
    final private ColumnStore store;
    final private LuceneStore lucene;
    final private Map<String, List<JSONObject>> catalogCache;
    public static long stat_index_c = 0;

    // entry metadata labels
    final private static String ENTRY_METADATA__PROPS = "p";

    // stream markers
    final private static String END_OF_RESULTS = "$end_of_results";
    final private static String END_OF_TABLE = "$end_of_table";
    final private static String END_OF_INFO = "$end_of_info";
    final private static String EMPTY_STRING = "";

    public RaptorIndex(String dataDir) throws Exception {
        catalogCache = new Builder<String, List<JSONObject>>()
                .initialCapacity(150000)
                .maximumWeightedCapacity(150000)
                .concurrencyLevel(Runtime.getRuntime().availableProcessors() * 4)
                .build();
        log.info("opening raptor-db & log");
        store = new ColumnStore(dataDir + "/raptor-db", 1);
        log.info("opening raptor-catalog");
        lucene = new LuceneStore(dataDir + "/raptor-catalog");
        Thread t = new Thread(new Runnable() {
            public void run() {
                log.info("starting lucene sync thread");
                int store_commit_ct = 0;
                while (true) {
                    try {
                        if (RaptorServer.shuttingDown) {
                            log.info("shutting down, terminating RaptorIndex thread");
                            return;
                        }
                        store_commit_ct++;
                        if (STORE_COMMIT_INTERVAL * store_commit_ct >=
                                LUCENE_COMMIT_INTERVAL) {
                            store_commit_ct = 0;
                            log.info("lucene.sync()");
                            lucene.sync();
                        }
                        if (stat_index_c > 0) {
                            log.info(">> " + stat_index_c + " operations");
                        }
                        stat_index_c = 0;
                        Thread.sleep(STORE_COMMIT_INTERVAL);
                    } catch (Exception ex) {
                        ex.printStackTrace();
                    }
                }
            }
        });
        t.start();
    }

    public void close() throws Exception {
        store.close();
        lucene.close();
    }

    public void index(String index,
                      String field,
                      String term,
                      String docId,
                      String partition,
                      byte[] props,
                      String keyClock) throws Exception {
        String table = makeTableKey(index, field, term, partition);
        if (!store.tableExists(table)) {
            addCatalogEntry(partition, index, field, term, null);
            String catalogTableKey = makeTableKey(index, field, term, partition);
            store.put(CATALOG_TABLE, catalogTableKey, term);
        }
        store.put(table, docId.getBytes("UTF-8"), keyClock.getBytes("UTF-8"));
        store.setTermEntryMetadata(table,
                docId.getBytes("UTF-8"),
                term,
                ENTRY_METADATA__PROPS,
                props);
        stat_index_c++;
    }

    public void indexIfNewer(String index,
                               String field,
                               String term,
                               String docId,
                               String partition,
                               byte[] props,
                               String updKeyClock) throws Exception {
        String table = makeTableKey(index, field, term, partition);
        String curKeyClock = null;
        if (!store.tableExists(table)) {
            addCatalogEntry(partition, index, field, term, null);
            String catalogTableKey = makeTableKey(index, field, term, partition);
            store.put(CATALOG_TABLE, catalogTableKey, term);
        } else {
            byte[] curKeyClockBytes = store.get(table, docId.getBytes("UTF-8"));
            if (curKeyClockBytes != null) {
                curKeyClock = new String(curKeyClockBytes);
            }
        }

        if (curKeyClock == null || is_newer(updKeyClock, curKeyClock)) {
            store.put(table, docId.getBytes("UTF-8"), updKeyClock.getBytes("UTF-8"));
            store.setTermEntryMetadata(table,
                    docId.getBytes("UTF-8"),
                    term,
                    ENTRY_METADATA__PROPS,
                    props);
            stat_index_c++;
        }
    }

    public String getEntryKeyClock(String index,
                                   String field,
                                   String term,
                                   String docId,
                                   String partition) throws Exception {
        String table = makeTableKey(index, field, term, partition);
        byte[] keyClockBytes = store.get(table, docId.getBytes("UTF-8"));
        if (keyClockBytes == null) return "0";
        return new String(keyClockBytes, "UTF-8");
    }

    public void deleteEntry(String index,
                            String field,
                            String term,
                            String docId,
                            String partition) throws Exception {
        String table = makeTableKey(index, field, term, partition);
        boolean res = store.delete(table, docId);
        boolean res1 = store.deleteTermEntryMetadata(table,
                docId.getBytes("UTF-8"),
                term,
                ENTRY_METADATA__PROPS);
        if (!res || !res1) {
            log.info("delete(" + index + ", " + field + ", " + term + ", " + docId + ", " +
                    partition + "), [index] res = " + res + ", [entry metadata] res1 = " + res1);
        }
    }

    public void stream(final String index,
                       final String field,
                       final String term,
                       final String partition,
                       final ResultHandler resultHandler) throws Exception {
        final String table = makeTableKey(index, field, term, partition);
        Map<byte[], byte[]> results =
                store.getRange(table,
                        ("").getBytes("UTF-8"),
                        ("").getBytes("UTF-8"),
                        true,
                        true,
                        new ResultHandler() {
                            public void handleResult(byte[] key, byte[] key_clock) {
                                try {
                                    // value: KeyClock
                                    byte[] props = store.getTermEntryMetadata(table,
                                            key,
                                            term,
                                            ENTRY_METADATA__PROPS);
                                    if (props != null)
                                        resultHandler.handleResult(key, props, key_clock);
                                } catch (Exception ex) {
                                    ex.printStackTrace();
                                }
                            }
                        });
        resultHandler.handleResult(END_OF_TABLE.getBytes(),
                                   EMPTY_STRING.getBytes(),
                                   EMPTY_STRING.getBytes());
    }

    public void multistream(JSONArray terms,
                            final ResultHandler resultHandler)
            throws Exception {
        long ctime = System.currentTimeMillis();
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < terms.length(); i++) {
            JSONObject jo = terms.getJSONObject(i);
            sb.append("(index:\"");
            sb.append(jo.getString("index"));
            sb.append("\" AND field:\"");
            sb.append(jo.getString("field"));
            sb.append("\" AND term:\"");
            sb.append(jo.getString("term"));
            sb.append("\")");
            if (i < (terms.length() - 1)) sb.append(" OR ");
        }
        String query = sb.toString();
        final List<JSONObject> catalogEntries;
        if (catalogCache.get(query) == null) {
            catalogEntries = new ArrayList<JSONObject>();
            lucene.query(query, new ResultHandler() {
                public void handleCatalogResult(JSONObject obj) {
                    catalogEntries.add(obj); // partition_id, index, field, term
                }
            });
            catalogCache.put(query, catalogEntries);
        } else {
            catalogEntries = catalogCache.get(query);
        }

        log.info("catalogTime = " + (System.currentTimeMillis() - ctime));
        HashSet<String> completeTerms = new HashSet<String>();
        List<String> tables = new ArrayList<String>();
        Map<String, String> tableTerms = new HashMap<String, String>();

        for (JSONObject catalogEntry : catalogEntries) {
            final String index = catalogEntry.getString("index");
            final String field = catalogEntry.getString("field");
            final String term = catalogEntry.getString("term");
            final String partition = catalogEntry.getString("partition_id");
            final String table = makeTableKey(index, field, term, partition);
            String cTermStr = index + "." + field + "." + term;
            if (completeTerms.contains(cTermStr)) continue;
            completeTerms.add(cTermStr);
            tables.add(table);
            tableTerms.put(table, term);
        }
        Collections.sort(tables);

        for (final String table : tables) {
            long t1 = System.currentTimeMillis();
            final String term = tableTerms.get(table);
            Map<byte[], byte[]> results =
                    store.getRange(table,
                            ("").getBytes("UTF-8"),
                            ("").getBytes("UTF-8"),
                            true,
                            true,
                            new ResultHandler() {
                                public void handleResult(byte[] key, byte[] value) {
                                    try {
                                        byte[] props = store.getTermEntryMetadata(table,
                                                key,
                                                term,
                                                ENTRY_METADATA__PROPS);
                                        if (props != null) {
                                            resultHandler.handleResult(key, props);
                                        }
                                    } catch (Exception ex) {
                                        ex.printStackTrace();
                                    }
                                }
                            });
        }
        long elapsed = System.currentTimeMillis() - ctime;
        log.info("<< multistream complete, " + elapsed + "ms elapsed >>");
        resultHandler.handleResult(END_OF_TABLE.getBytes(),
                                   EMPTY_STRING.getBytes());
    }

    public void info(String index,
                     String field,
                     String term,
                     String partition,
                     ResultHandler resultHandler) throws Exception {
        String table = makeTableKey(index, field, term, partition);
        long n = store.count(table);
        resultHandler.handleInfoResult(table, n);
        resultHandler.handleInfoResult(END_OF_INFO, 0);
    }

    public void infoRange(String index,
                          String field,
                          String startTerm,
                          String endTerm,
                          String partition,
                          ResultHandler resultHandler) throws Exception {
        String fromTable = makeTableKey(index, field, startTerm, partition);
        String toTable = makeTableKey(index, field, endTerm, partition);
        Map<String, String> results =
                store.getRange(CATALOG_TABLE,
                        fromTable,
                        toTable,
                        true, true);
        for (String k : results.keySet()) {
            String v = results.get(k);
            long c = store.count(k);
            resultHandler.handleInfoResult(v, c);
        }
        resultHandler.handleInfoResult(END_OF_INFO, 0);
    }

    public void catalogQuery(String query,
                             long maxResults,
                             ResultHandler resultHandler) throws Exception {
        lucene.query(query, resultHandler);
        JSONObject jo = new JSONObject();
        jo.put("partition_id", END_OF_RESULTS);
        jo.put("index", "");
        jo.put("field", "");
        jo.put("term", "");
        resultHandler.handleCatalogResult(jo);
    }

    public synchronized void shutdown() throws Exception {
        sync();
        close();
    }

    // only supposed to be used externally
    public synchronized void sync() throws Exception {
        lucene.sync();
    }

    // deletes a partition and its keys
    // todo: clean table entries from system catalog
    public void dropPartition(String partition) throws Exception {
        List<byte[]> keys =
                store.getRawKeys(partition + TABLE_SEP,
                                 "", "",
                                 true,
                                 true);
        int k_total = 0, k_actual = 0;
        for (byte[] k : keys) {
            k_total++;
            log.info("dropPartition(" + partition + ") delete: " + new String(k, "UTF-8"));
            if (store.rawDelete(k)) k_actual++;
        }
        log.info("dropPartition(" + partition + ") done: " + k_actual + " out of " + k_total + " deleted");
    }

    // # records in a given partition
    public int partitionCount(String partition) throws Exception {
        return store.countRange(partition + TABLE_SEP, "", "", true, true);
    }

    private String makeTableKey(String index,
                                String field,
                                String term,
                                String partition) throws Exception {
        return partition + TABLE_SEP +
                index + TABLE_SEP +
                field + TABLE_SEP +
                term;
    }

    private String makeCatalogTableKey(String index,
                                       String field,
                                       String term,
                                       String partition) throws Exception {
        return partition + IFT_SEP +
                index + IFT_SEP +
                field + IFT_SEP +
                term;
    }

    private void addCatalogEntry(String partitionId,
                                 String index,
                                 String field,
                                 String term,
                                 JSONObject attrs) throws Exception {
        Document doc = new Document();
        doc.add(new Field("partition_id",
                partitionId,
                Field.Store.YES,
                Field.Index.NOT_ANALYZED_NO_NORMS));
        doc.add(new Field("index",
                index,
                Field.Store.YES,
                Field.Index.ANALYZED_NO_NORMS));
        doc.add(new Field("field",
                field,
                Field.Store.YES,
                Field.Index.ANALYZED_NO_NORMS));
        doc.add(new Field("term",
                term,
                Field.Store.YES,
                Field.Index.ANALYZED_NO_NORMS));

        if (attrs != null) {
            for (String key : JSONObject.getNames(attrs)) {
                doc.add(new Field(key, attrs.getString(key),
                        Field.Store.YES, Field.Index.ANALYZED_NO_NORMS));
            }
        }

        lucene.addDocument(doc);
    }

    private boolean is_newer(String updKeyClock, String curKeyClock) {
        return Integer.getInteger(updKeyClock) > Integer.getInteger(curKeyClock);
    }

    public static void main(String args[]) throws Exception {
        RaptorIndex idx = new RaptorIndex("");

        ResultHandler handler =
                new ResultHandler() {
                    public void handleResult(byte[] key, byte[] value) {
                        log.info("<byte[], byte[]> handleResult(" + Arrays.toString(key) + ", " + Arrays.toString(value) + ")");
                    }

                    public void handleInfoResult(String bucket, long count) {
                        log.info("handleInfoResult(" + bucket + ", " + count + ")");
                    }
                };

        idx.index("search", "payload", "test", "docid_1", "1", new byte[0], "1234");
        idx.index("search", "payload", "test", "docid_2", "1", new byte[0], "1234");
        idx.index("search", "payload", "test", "docid_3", "1", new byte[0], "1234");
        idx.index("search", "payload", "test", "docid_4", "1", new byte[0], "1234");
        idx.index("search", "payload", "test", "docid_5", "1", new byte[0], "1234");

        idx.index("search", "payload", "funny", "docid_1", "1", new byte[0], "1234");
        idx.index("search", "payload", "funny", "docid_2", "1", new byte[0], "1234");
        idx.index("search", "payload", "funny", "docid_3", "1", new byte[0], "1234");
        idx.index("search", "payload", "funny", "docid_4", "1", new byte[0], "1234");
        idx.index("search", "payload", "funny", "docid_5", "1", new byte[0], "1234");

        idx.index("search", "payload", "bananas", "docid_1", "1", new byte[0], "1234");
        idx.index("search", "payload", "bananas", "docid_2", "1", new byte[0], "1234");
        idx.index("search", "payload", "bananas", "docid_3", "1", new byte[0], "1234");
        idx.index("search", "payload", "bananas", "docid_4", "1", new byte[0], "1234");
        idx.index("search", "payload", "bananas", "docid_5", "1", new byte[0], "1234");

        idx.stream("search", "payload", "funny", "1", handler);
        idx.info("search", "payload", "test", "1", handler);
        idx.infoRange("search", "payload", "apples", "ferrari", "1", handler);
        idx.infoRange("search", "payload", "energizer", "zebra", "1", handler);

    }

}
