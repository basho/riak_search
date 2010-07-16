// -------------------------------------------------------------------
//
// Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
//
// -------------------------------------------------------------------

package raptor.util;

import java.util.*;
import java.io.*;
import java.net.URL;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class RaptorUtils {
	final private static Logger log = Logger.getLogger(RaptorUtils.class);
	
    public static File ensureDirectory(String directory) throws Exception {
        File f = new File(directory);
        if (f.exists()) return f;
        if (!f.mkdirs()) {
            throw new Exception("ensureDirectory: Cannot create directory: " +
                    directory);
        }
        return f;
    }

    public static ConsistentHash createConsistentHash(String pfx, int sz) throws Exception {
        List<String> ar = new ArrayList<String>();
        for (int i = 0; i < sz; i++) {
            String s = pfx + i;
            ar.add(s);
        }
        return new ConsistentHash<String>(1, ar);
    }
    
    public static ConsistentHash<String> createConsistentHash(String[] pfx) throws Exception {
        List<String> ar = new ArrayList<String>();
        ar.addAll(Arrays.asList(pfx));
        return new ConsistentHash<String>(1, ar);
    }
    
    public static ConsistentHash<String> createConsistentHash(List<String> pfx) throws Exception {
        return new ConsistentHash<String>(1, pfx);
    }
    
    public static String getenv(String s) {
        if (System.getenv(s) == null) return System.getProperty(s);
        else return System.getenv(s);
    }
    
        public static String getFileContents(String fn) throws Exception {
        return getFileContents(new File(fn));
    }

    public static String getFileContents(File aFile) throws Exception {
        StringBuilder contents = new StringBuilder();
        BufferedReader input = new BufferedReader(new FileReader(aFile));
        try {
            String line;
            while ((line = input.readLine()) != null) {
                contents.append(line);
                contents.append(System.getProperty("line.separator"));
            }
        } finally {
            input.close();
        }
        return contents.toString();
    }

    public static void setFileContents(String fn, String s)
            throws IOException {
        setFileContents(new File(fn), s);
    }

    public static void setFileContents(File aFile, String aContents)
            throws IOException {
        Writer output = new BufferedWriter(new FileWriter(aFile));
        try {
            output.write(aContents);
        }
        finally {
            output.close();
        }
    }

    public static String SHA1(String text)
            throws NoSuchAlgorithmException, UnsupportedEncodingException {
        MessageDigest md;
        md = MessageDigest.getInstance("SHA-1");
        byte[] sha1hash = new byte[40];
        md.update(text.getBytes("iso-8859-1"), 0, text.length());
        sha1hash = md.digest();
        return convertToHex(sha1hash);
    }

    public static String MD5(String text)
            throws NoSuchAlgorithmException, UnsupportedEncodingException {
        MessageDigest md;
        md = MessageDigest.getInstance("MD5");
        byte[] sha1hash = new byte[40];
        md.update(text.getBytes("iso-8859-1"), 0, text.length());
        sha1hash = md.digest();
        return convertToHex(sha1hash);
    }

    public static String generateUUID() {
        return UUID.randomUUID().toString();
    }

    public static String convertToHex(byte[] data) {
        StringBuffer buf = new StringBuffer();
        for (byte aData : data) {
            int halfbyte = (aData >>> 4) & 0x0F;
            int two_halfs = 0;
            do {
                if ((0 <= halfbyte) && (halfbyte <= 9))
                    buf.append((char) ('0' + halfbyte));
                else
                    buf.append((char) ('a' + (halfbyte - 10)));
                halfbyte = aData & 0x0F;
            } while (two_halfs++ < 1);
        }
        return buf.toString();
    }

}
