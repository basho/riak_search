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

import java.util.Random;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;

import raptor.store.bdb.DefaultBDBMessageHandler;

import com.sleepycat.db.Database;
import com.sleepycat.db.DatabaseConfig;
import com.sleepycat.db.DatabaseEntry;
import com.sleepycat.db.DatabaseType;
import com.sleepycat.db.Environment;
import com.sleepycat.db.EnvironmentConfig;
import com.sleepycat.db.OperationStatus;

public class QueueStore {
    final private static Logger log = 
        Logger.getLogger(QueueStore.class);
    private DatabaseConfig databaseConfig;
    private Database db;
    private EnvironmentConfig environmentConfig;
    private Environment env;
    private Lock dbLock;
    private String filename;
    private String name;

    private DefaultBDBMessageHandler 
        defaultMessageHandler = new DefaultBDBMessageHandler();
    
    public QueueStore(String filename, String name, int extentSize, int recordLength) 
        throws Exception {
        dbLock = new ReentrantLock();
        this.filename = filename;
        this.name = name;
        databaseConfig = new DatabaseConfig();
        databaseConfig.setAllowCreate(true);
        databaseConfig.setErrorStream(System.err);
        databaseConfig.setErrorHandler(defaultMessageHandler);
        databaseConfig.setFeedbackHandler(defaultMessageHandler);
        databaseConfig.setMessageHandler(defaultMessageHandler);
        databaseConfig.setErrorPrefix("<" + filename + ": " + name + "> ");
        databaseConfig.setType(DatabaseType.QUEUE);
        databaseConfig.setChecksum(true);
        databaseConfig.setPageSize(4096);
        databaseConfig.setQueueInOrder(true);
        databaseConfig.setQueueExtentSize(extentSize);
        databaseConfig.setRecordLength(recordLength);
        db = new Database(filename, null, databaseConfig);
    }
    
    public void close() throws Exception {
        dbLock.lock();
        try {
            db.close();
        } finally {
            dbLock.unlock();
        }
    }
    
    public void append(String val) throws Exception {
        append(val.getBytes("UTF-8"));
    }
    
    public void append(byte[] val) throws Exception {
        DatabaseEntry dbKey = new DatabaseEntry();
        dbLock.lock();
        try {
            db.append(null,
                dbKey,
                new DatabaseEntry(val));
        } finally {
            dbLock.unlock();
        }
    }
    
    public String consume() throws Exception {
        byte[] v = consume(true);
        if (v == null) return null;
        return new String(v, "UTF-8");
    }
    
    public byte[] consume(boolean wait) throws Exception {
        DatabaseEntry dbKey = new DatabaseEntry();
        DatabaseEntry dbVal = new DatabaseEntry();
        if (db.consume(null, dbKey, dbVal, wait) == 
            OperationStatus.SUCCESS) {
            return dbVal.getData();
        } else {
            return null;
        }
    }
    
    public void sync() throws Exception {
        dbLock.lock();
        try {
            db.sync();
        } finally {
            dbLock.unlock();
        }
    }
    
    public static void main(String args[]) throws Exception {
        // 10000 record extents,
        // 1024 byte record length
        QueueStore store = new QueueStore("test.queue", "test", 10000, 1024);
        
        Random r = new Random();
        long startTime = System.currentTimeMillis();
        for(int i=0; i<1000000; i++) {
            String v = "v" + r.nextInt(1929398);
            store.append(v);
        }
        store.sync();
        log.info("1m appends in " + ( (System.currentTimeMillis() - startTime) / 1000 ) + " sec");
        
        log.info("starting reads...");
        startTime = System.currentTimeMillis();
        for(int i=0; i<1000000; i++) {
            store.consume();
        }
        log.info("1m consumes in " + ( (System.currentTimeMillis() - startTime) / 1000 ) + " sec");
        
        store.close();
    }
}
