// -------------------------------------------------------------------
//
// Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
//
// -------------------------------------------------------------------

package raptor.util;

import java.util.LinkedHashMap;
import java.util.Collection;
import java.util.Map;
import java.util.ArrayList;

public class LRUCache<K,V> {
    final private static float hashTableLoadFactor = 0.75f;
    private LinkedHashMap<K,V> map;
    private int cacheSize;

    public LRUCache (int cacheSize) {
       this.cacheSize = cacheSize;
       int hashTableCapacity = (int)Math.ceil(cacheSize / hashTableLoadFactor) + 1;
       map = new LinkedHashMap<K,V>(hashTableCapacity, hashTableLoadFactor, true) {
          private static final long serialVersionUID = 1;
          @Override protected boolean removeEldestEntry (Map.Entry<K,V> eldest) {
             return size() > LRUCache.this.cacheSize; 
          }
       }; 
    }
    
    public V get (K key) {
       return map.get(key); }
    
    public void put (K key, V value) {
       map.put (key,value); }
    
    public void clear() {
       map.clear(); }
    
    public int usedEntries() {
       return map.size(); }

    public Collection<Map.Entry<K,V>> getAll() {
       return new ArrayList<Map.Entry<K,V>>(map.entrySet()); }
}