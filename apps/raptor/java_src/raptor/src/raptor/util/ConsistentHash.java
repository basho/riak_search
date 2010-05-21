package raptor.util;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.SortedMap;
import java.util.TreeMap;

public class ConsistentHash<T> {
    private final int numberOfReplicas;
    private final SortedMap<BigInteger, T> ring =
            new TreeMap<BigInteger, T>();

    public ConsistentHash(int numberOfReplicas,
                    Collection<T> nodes) throws Exception {
        this.numberOfReplicas = numberOfReplicas;
        for (T node : nodes) {
            add(node);
        }
    }

    public void add(T node) throws Exception {
        for (int i = 0; i < numberOfReplicas; i++) {
            ring.put(hashFun(node.toString() + i), node);
        }
    }

    public void remove(T node) throws Exception {
        for (int i = 0; i < numberOfReplicas; i++) {
            ring.remove(hashFun(node.toString() + i));
        }
    }

    public T get(Object key) throws Exception {
        if (ring.isEmpty()) {
            return null;
        }
        BigInteger hash = hashFun(key);
        if (!ring.containsKey(hash)) {
            SortedMap<BigInteger, T> tailMap = ring.tailMap(hash);
            hash = tailMap.isEmpty() ? ring.firstKey() : tailMap.firstKey();
        }
        return ring.get(hash);
    }

    private BigInteger hashFun(Object key) throws Exception {
        String text = key.toString();
        MessageDigest md = MessageDigest.getInstance("MD5");
        byte[] hash_bytes = new byte[40];
        md.update(text.getBytes("UTF-8"), 0, text.length());
        hash_bytes = md.digest();
        BigInteger h = new BigInteger(1, hash_bytes);
        return h;
    }

    public static void main(String args[]) throws Exception {
        int i;
        List<String> ar = new ArrayList<String>();
        for (i = 0; i < 1024; i++) {
            String s = "table" + i;
            ar.add(s);
        }
        ConsistentHash<String> ch = new ConsistentHash<String>(1, ar);
        Random r = new Random(42);
        for (i = 0; i < 100; i++) {
            int z = r.nextInt(300023);
            String x = "test" + z;
            System.out.println(x + " => " + ch.get(x));
        }
    }
}

