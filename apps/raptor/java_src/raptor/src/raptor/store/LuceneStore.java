// -------------------------------------------------------------------
//
// Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
//
// -------------------------------------------------------------------

package raptor.store;

import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Fieldable;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.*;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.store.NativeFSLockFactory;
import org.apache.lucene.util.Version;
import org.json.JSONObject;
import raptor.store.handlers.ResultHandler;
import raptor.util.RaptorUtils;

import java.io.File;
import java.lang.Exception;
import java.lang.Runnable;
import java.lang.String;
import java.lang.SuppressWarnings;
import java.lang.Thread;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class LuceneStore {
    final private static Logger log = Logger.getLogger(LuceneStore.class);
    
    final private static int LUCENE_MERGE_FACTOR = 10;
    final private static boolean IDX_TRACE = false;
    final private static int MAX_RESULTS = 50000; // todo: configurable

    private IndexWriter luceneWriter;

    public LuceneStore(String directory) throws Exception {
        File luceneFS = RaptorUtils.ensureDirectory(directory);
        NIOFSDirectory luceneDirectory = new NIOFSDirectory(luceneFS);
        luceneDirectory.setLockFactory(
                new NativeFSLockFactory("/tmp")); // todo: configurable
        luceneWriter = new IndexWriter(
                luceneDirectory,
                new WhitespaceAnalyzer(),
                IndexWriter.MaxFieldLength.LIMITED);
        luceneWriter.setUseCompoundFile(false);
        luceneWriter.setRAMBufferSizeMB(500.0);
        luceneWriter.setMergeFactor(LUCENE_MERGE_FACTOR); // todo: config
        if (IDX_TRACE) luceneWriter.setInfoStream(System.err); // todo: config
    }

    public void addDocument(Document doc) throws Exception {
    	luceneWriter.addDocument(doc);
    }

    public void close() throws Exception {
    	luceneWriter.close();
    }

    public void sync() throws Exception {
    	luceneWriter.waitForMerges();
    	luceneWriter.commit();
    }

    public boolean documentsExist(String query)
            throws Exception {
        Analyzer analyzer = new WhitespaceAnalyzer();
        QueryParser qp = new QueryParser(Version.LUCENE_CURRENT,
                "bucket", analyzer);
        qp.setAllowLeadingWildcard(true);
        Query l_query = qp.parse(query);
        Filter filter = new QueryWrapperFilter(l_query);
        return documentsExist(filter);
    }

    protected boolean documentsExist(Filter f1) throws Exception {
        TopDocs hits;
        Filter f = new CachingWrapperFilter(f1);
        IndexReader reader = null;
        IndexSearcher searcher = null;
        try
        {
            reader = luceneWriter.getReader();
            searcher = new IndexSearcher(reader);
            hits = searcher.search(new org.apache.lucene.search.MatchAllDocsQuery(), f, 1);
            return hits.scoreDocs.length > 0;
        }
        finally
        {
        	if (searcher != null) { searcher.close(); }
        	if (reader != null) { reader.close(); }
        }
    }

    public List<Document> query(String query)
            throws Exception {
        return query(query, 0);
    }

    public void query(String query,
                      final ResultHandler resultHandler)
            throws Exception {
        Analyzer analyzer = new WhitespaceAnalyzer();
        QueryParser qp = new QueryParser(Version.LUCENE_CURRENT,
                "index_field", analyzer);
        qp.setAllowLeadingWildcard(true);
        Query l_query = qp.parse(query);
        Filter l_filter = new CachingWrapperFilter(new QueryWrapperFilter(l_query));
        
        IndexReader reader = null;
        IndexSearcher searcher = null;

        try {
            reader = luceneWriter.getReader();
            searcher = new IndexSearcher(reader);

            searcher.search(
                    new MatchAllDocsQuery(),
                    l_filter,
                    new Collector() {
                        private int docBase;
                        private IndexReader reader;

                        public void setScorer(Scorer scorer) { }

                        public boolean acceptsDocsOutOfOrder() {
                            return true;
                        }

                        public void collect(int docn) {
                            try {
                                Document doc = reader.document(docn);
                                JSONObject jo = new JSONObject();
                                List<Fieldable> fields = doc.getFields();
                                for (Fieldable field : fields) {
                                    jo.put(field.name(), field.stringValue());
                                }
                                resultHandler.handleCatalogResult(jo);
                            } catch (Exception ex) {
                                ex.printStackTrace();
                            }
                        }

                        public void setNextReader(IndexReader reader, int docBase) {
                            this.docBase = docBase;
                            this.reader = reader;
                        }
                    }
            );
        } 
        finally 
        {
        	if (searcher != null) { searcher.close(); }
        	if (reader != null) { reader.close(); }
        }
    }

    public List<Document> query(String query, int maxResults)
            throws Exception {
        List<Document> matches = new ArrayList<Document>();
        Analyzer analyzer = new WhitespaceAnalyzer();
        QueryParser qp = new QueryParser(Version.LUCENE_CURRENT, "index_field", analyzer);
        qp.setAllowLeadingWildcard(true);
        Query l_query = qp.parse(query);
        Filter l_filter = new CachingWrapperFilter(new QueryWrapperFilter(l_query));

        IndexReader reader = null;
        IndexSearcher searcher = null;
        try {
            reader = luceneWriter.getReader();
            searcher = new IndexSearcher(reader);

            TopDocs hits;
            if (maxResults == 0) {
                hits = searcher.search(new MatchAllDocsQuery(), l_filter, Integer.MAX_VALUE - 1);
            } else {
                hits = searcher.search(new MatchAllDocsQuery(), l_filter, MAX_RESULTS);
            }
            for (int i = 0; i < hits.scoreDocs.length; i++) {
                int docId = hits.scoreDocs[i].doc;
                Document d = searcher.doc(docId);
                matches.add(d);
            }
        } 
        finally 
        {
        	if (searcher != null) { searcher.close(); }
        	if (reader != null) { reader.close(); }
        }
        return matches;
    }
}

