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

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.sleepycat.db.*;
import org.apache.log4j.Logger;
import org.json.*;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import raptor.util.*;
import raptor.store.bdb.*;

public class HashStore {
    final private static Logger log = 
        Logger.getLogger(HashStore.class);
    private DatabaseConfig databaseConfig;
    private Database db;
    private EnvironmentConfig environmentConfig;
    private Environment env;
    private Lock dbLock;
    private String filename;
    private String name;

    private DefaultBDBMessageHandler 
        defaultMessageHandler = new DefaultBDBMessageHandler();
        
    private Thread writeThread;
    protected LinkedBlockingQueue<Object> writeQueue;

    public HashStore(String filename, String name) 
        throws Exception {
        this(BtreeStore.getDefaultEnvironment(".", "."), filename, name);
    }
    
    public HashStore(String directory, String filename, String name) 
        throws Exception {
        this(BtreeStore.getDefaultEnvironment(directory, directory), filename, name);
    }
    
    public HashStore(String directory, String logDirectory, String filename, String name) 
        throws Exception {
        this(BtreeStore.getDefaultEnvironment(directory, logDirectory), filename, name);
    }
    
    public HashStore(Environment env, String filename, String name) 
        throws Exception {
        dbLock = new ReentrantLock();
        this.env = env;
        this.filename = filename;
        this.name = name;
        databaseConfig = new DatabaseConfig();
        databaseConfig.setAllowCreate(true);
        databaseConfig.setErrorStream(System.err);
        databaseConfig.setErrorHandler(defaultMessageHandler);
        databaseConfig.setFeedbackHandler(defaultMessageHandler);
        databaseConfig.setMessageHandler(defaultMessageHandler);
        databaseConfig.setErrorPrefix("<" + filename + ": " + name + "> ");
        databaseConfig.setType(DatabaseType.HASH);
        databaseConfig.setChecksum(true);        
        databaseConfig.setTransactional(false);
        //databaseConfig.setPageSize(4096);
        databaseConfig.setReadUncommitted(true);
        
        db = env.openDatabase(null, filename, name, databaseConfig);
        startWriteThread();
    }
    
    public void close() throws Exception {
        dbLock.lock();
        try {
            db.close();
        } finally {
            dbLock.unlock();
        }
    }
    
    private void startWriteThread() {
       writeQueue = new LinkedBlockingQueue<Object>();
       writeThread = new Thread(new Runnable() {
           public void run() {
               try {
                   log.info("writeThread: started");
                   while(true) {
                       try {
                           //Object msg0 = writeQueue.take();
                           while(writeQueue.size() == 0) {
                               Thread.sleep(5);
                           }
                           List<Object> batch = new ArrayList<Object>();
                           writeQueue.drainTo(batch); // todo: configurable batch size?
                           for(Object msg0: batch) {
                               if (msg0 instanceof PutOp) {
                                   PutOp msg = (PutOp) msg0;
                                   do_put(msg.key, msg.val);
                               } else if (msg0 instanceof DeleteOp) {
                                   DeleteOp msg = (DeleteOp) msg0;
                                   do_delete(msg.key);
                               } else {
                                   log.error("writeQueue: unknown message (discarding): " + 
                                             msg0.toString());
                               }
                           }
                       } catch (Exception iex) {
                           log.error("Error handling message", iex);
                           iex.printStackTrace();
                       }
                   }
               } catch (Exception ex) {
                   log.error("Error setting up writeQueue process", ex);
                   ex.printStackTrace();
               }
           }
       });
       
       try {
           writeThread.start();
       } catch (Exception ex) {
           log.error("Error starting BtreeStore writeThread", ex);
           System.exit(-1);
       }
    }
    
    /* * */
    
    private class PutOp { public byte[] key, val; }
    private class DeleteOp { public byte[] key; }    
    
    public boolean put(String key, String val) throws Exception {
        return put(key.getBytes("UTF-8"), val.getBytes("UTF-8"));
    }
    
    public boolean put(String key, byte[] val) throws Exception {
        return put(key.getBytes("UTF-8"), val);
    }
    
    public boolean put(byte[] key, byte[] val) throws Exception {
        PutOp op = new PutOp();
        op.key = key;
        op.val = val;
        writeQueue.put(op);
        return true;
    }

    
    public boolean do_put(byte[] key, byte[] val) throws Exception {
        dbLock.lock();
        try {
            if (db.put(null,
                new DatabaseEntry(key),
                new DatabaseEntry(val)) == OperationStatus.SUCCESS) {
                return true;
            }
        } finally {
            dbLock.unlock();
        }
        return false;
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
            //log.info("error: hashdb: [" + new String(key, "UTF-8") + "] not found");
            return null;
        }
    }
    
    public void sync() throws Exception {
        //dbLock.lock();
        try {
            db.sync();
        } finally {
            //dbLock.unlock();
        }
    }

    public boolean delete(byte[] key) throws Exception {
        DeleteOp op = new DeleteOp();
        op.key = key;
        writeQueue.put(op);
        return true;
    }

    public boolean do_delete(byte[] key) throws Exception {
        dbLock.lock();
        try {
            if (db.delete(null, new DatabaseEntry(key)) ==
                OperationStatus.SUCCESS) return true;
        } finally {
            dbLock.unlock();
        }
        return false;
    }

    public boolean exists(byte[] key) throws Exception {
        dbLock.lock();
        try {
            if (db.exists(null, new DatabaseEntry(key)) ==
                OperationStatus.SUCCESS) return true;
        } finally {
            dbLock.unlock();
        }
        return false;
    }
    
    
    
    public static void main(String args[]) throws Exception {
        HashStore store = new HashStore("test.hash", "test");
        store.put("apple", "fruit: apple");
        store.put("banana", "fruit: banana");
        store.put("carrot", "vegetable: carrot");
        store.put("potato", "vegetable: potato");
        store.put("pineapple", "fruit: pineapple");
        
        Random r = new Random();
        long startTime = System.currentTimeMillis();
        for(int i=0; i<1000000; i++) {
            String k = "k" + i;
            String v = "v" + r.nextInt(1929398);
            store.put(k, v);
        }
        store.sync();
        log.info("1m writes in " + ( (System.currentTimeMillis() - startTime) / 1000 ) + " sec");
        log.info("starting reads...");
        startTime = System.currentTimeMillis();
        for(int i=0; i<1000000; i++) {
            String k = "k" + i;
            String v = store.get(k);
        }
        log.info("1m reads in " + ( (System.currentTimeMillis() - startTime) / 1000 ) + " sec");
        
        log.info("store.get(potato): " + store.get("potato"));
        store.close();
    }
}
