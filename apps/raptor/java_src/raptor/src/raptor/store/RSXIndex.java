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

import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.json.JSONObject;

import raptor.server.RaptorServer;
import raptor.store.handlers.ResultHandler;

public class RSXIndex implements Runnable {
    final private static Logger log = 
        Logger.getLogger(RSXIndex.class);
    final private static String TABLE_SEP = "/";
    final private static String IFT_SEP = ".";
    final private static String CATALOG_TABLE = "__sys._catalog_";
    final private static int STORE_COMMIT_INTERVAL = 30000; /* ms */
    //final private static int STORE_SYNC_INTERVAL = 30000; /* ms */
    final private static int LUCENE_COMMIT_INTERVAL = 30000; /* ms */
    final private ColumnStore store;
    final private LuceneStore lucene;
    
    public static long stat_index_c = 0;
    
    public RSXIndex(String dataDir) throws Exception {
        store = new ColumnStore(dataDir + "/raptor-db", "log", 1);
        lucene = new LuceneStore(dataDir + "/raptor-catalog");
        RaptorServer.writeThread.start();
    }

    public void run() {
        int store_commit_ct = 0;
        while(true) {
            try {
                if (RaptorServer.shuttingDown) {
                    log.info("shutting down, terminating RSXIndex thread");
                    return;
                }
                long cpt = System.currentTimeMillis();
                store.checkpoint();
                store_commit_ct++;
                if (STORE_COMMIT_INTERVAL * store_commit_ct >= 
                    LUCENE_COMMIT_INTERVAL) {
                    store_commit_ct = 0;
                    lucene.sync();
                }
                long cpt2 = System.currentTimeMillis() - cpt;
                log.info("checkpoint: " + cpt2);
                int queueSize = RaptorServer.writeQueue.size();
                log.info(">> " + stat_index_c + " index puts (" + 
                    queueSize + " queued)");
                stat_index_c = 0;
                Thread.sleep(STORE_COMMIT_INTERVAL);
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
    }

    public void close() throws Exception {
        store.close();
        lucene.close();
    }
    
    public void index(String index,
                      String field,
                      String term,
                      String value,
                      String partition,
                      byte[] props) throws Exception {
        String table = makeTableKey(index, field, term, partition);
        if (!store.tableExists(table)) {
            addCatalogEntry(partition, index, field, term, null);
            String catalogTableKey = makeTableKey(index, field, term, partition);
            store.put(CATALOG_TABLE, catalogTableKey, term);
        }
        store.put(table, value.getBytes("UTF-8"), props); // todo: props
        stat_index_c++;
    }
    
    public void deleteEntry(String index,
                            String field,
                            String term,
                            String docId,
                            String partition) throws Exception {
        String table = makeTableKey(index, field, term, partition);
        boolean res = store.delete(table, docId);
        log.info("delete(" + index+ ", " + field + ", " + term + ", " + docId + ", " +
                 partition + "), res = " + res);
    }

    public void stream(String index,
                       String field,
                       String term,
                       String partition,
                       final ResultHandler resultHandler) throws Exception {
        String table = makeTableKey(index, field, term, partition);
        Map<byte[], byte[]> results = 
            store.getRange(table, 
                           ("").getBytes("UTF-8"), 
                           ("").getBytes("UTF-8"), 
                           true, 
                           true,
                           resultHandler);
        /*for(byte[] k: results.keySet()) {
            resultHandler.handleResult(k, results.get(k));
        }*/
        resultHandler.handleResult("$end_of_table", "");
    }
    
    public void info(String index,
                     String field,
                     String term,
                     String partition,
                     ResultHandler resultHandler) throws Exception {
        String table = makeTableKey(index, field, term, partition);
        long n = store.count(table);
        resultHandler.handleInfoResult(table, n);
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
        for(String k: results.keySet()) {
            String v = results.get(k);
            long c = store.count(k);
            resultHandler.handleInfoResult(v, c);
        }
        resultHandler.handleInfoResult("$end_of_info", 0);
    }
    
    public void catalogQuery(String query,
                             long maxResults,
                             ResultHandler resultHandler) throws Exception {
        lucene.query(query, resultHandler);
        JSONObject jo = new JSONObject();
        jo.put("partition_id", "$end_of_results");
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
    // fire and forget
    public synchronized void sync() throws Exception {
        log.info(" << sync() >> ");
        lucene.sync();
        store.sync();
    }
    
    // deletes a partition and its keys
    // fire and forget
    // todo: clean table entries from system catalog
    public void dropPartition(String partition) throws Exception {
        List<byte[]> keys = 
            store.getRawKeys(partition + TABLE_SEP, 
                             "", "",
                             true, true);
        int k_total=0, k_actual=0;
        for(byte[] k: keys) {
            k_total++;
            log.info("dropPartition(" +
                partition + ") delete: " + new String(k, "UTF-8"));
            if (store.rawDelete(k)) k_actual++;
        }
        log.info("dropPartition(" + 
            partition + ") done: " + k_actual + 
            " out of " + k_total + " deleted");
    }
    
    // # records in a given partition
    public int partitionCount(String partition) throws Exception {
        return store.countRange(partition + TABLE_SEP, 
            "", "", true, true);
    }
    
    /* -- */
    
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
    
    public static void main(String args[]) throws Exception {
        RSXIndex idx = new RSXIndex("");
        
        ResultHandler handler = 
            new ResultHandler() {
                    public void handleResult(byte[] key, byte[] value) {
                        log.info("<byte[], byte[]> handleResult(" + key + ", " + value + ")");
                    }
                    public void handleResult(String key, String value) {
                        log.info("<string, string> handleResult(" + key + ", " + value + ")");
                    }
                    public void handleInfoResult(String bucket, long count) {
                        log.info("handleInfoResult(" + bucket + ", " + count + ")");
                    }
            };
        
        idx.index("search", "payload", "test", "docid_1", "1", new byte[0]);
        idx.index("search", "payload", "test", "docid_2", "1", new byte[0]);
        idx.index("search", "payload", "test", "docid_3", "1", new byte[0]);
        idx.index("search", "payload", "test", "docid_4", "1", new byte[0]);
        idx.index("search", "payload", "test", "docid_5", "1", new byte[0]);
        
        idx.index("search", "payload", "funny", "docid_1", "1", new byte[0]);
        idx.index("search", "payload", "funny", "docid_2", "1", new byte[0]);
        idx.index("search", "payload", "funny", "docid_3", "1", new byte[0]);
        idx.index("search", "payload", "funny", "docid_4", "1", new byte[0]);
        idx.index("search", "payload", "funny", "docid_5", "1", new byte[0]);
        
        idx.index("search", "payload", "bananas", "docid_1", "1", new byte[0]);
        idx.index("search", "payload", "bananas", "docid_2", "1", new byte[0]);
        idx.index("search", "payload", "bananas", "docid_3", "1", new byte[0]);
        idx.index("search", "payload", "bananas", "docid_4", "1", new byte[0]);
        idx.index("search", "payload", "bananas", "docid_5", "1", new byte[0]);
        
        idx.stream("search", "payload", "funny", "1", handler);
        idx.info("search", "payload", "test", "1", handler);
        idx.infoRange("search", "payload", "apples", "ferrari", "1", handler);
        idx.infoRange("search", "payload", "energizer", "zebra", "1", handler);
        
    }
    
}