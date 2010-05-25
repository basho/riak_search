package raptor.store.bdb;

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

public class DefaultPartitionHandler implements com.sleepycat.db.PartitionHandler {
    
    private ConsistentHash chash;
    private HashMap<String, Integer> map;
    
    public DefaultPartitionHandler(String[] partitionDirs) throws Exception {
        map = new HashMap<String, Integer>();
        chash = RaptorUtils.createConsistentHash(partitionDirs);
        for(int i=0; i<partitionDirs.length; i++) {
            map.put(partitionDirs[i], new Integer(i));
        }
    }
    
    public int partition(Database db, DatabaseEntry key) {
        try {
            return map.get(chash.get(new String(key.getData())));
        } catch (Exception ex) {
            ex.printStackTrace();
            return 0;
        }
    }
}
