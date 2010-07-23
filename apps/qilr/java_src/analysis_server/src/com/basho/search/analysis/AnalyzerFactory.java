package com.basho.search.analysis;

import java.io.StringReader;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.util.Version;

public interface AnalyzerFactory {

   public TokenStream makeStream(Version version, StringReader input, String[] args);
}
