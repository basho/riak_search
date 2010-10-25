package com.basho.search.analysis;

import java.io.StringReader;

import org.apache.lucene.analysis.LengthFilter;
import org.apache.lucene.analysis.LowerCaseFilter;
import org.apache.lucene.analysis.StopAnalyzer;
import org.apache.lucene.analysis.StopFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.util.Version;

public class DefaultAnalyzerFactory implements AnalyzerFactory {

   public TokenStream makeStream(Version version, StringReader input, String[] args) {
      TokenStream stream = new StandardTokenizer(version, input);
      stream = new LengthFilter(stream, 3, Integer.MAX_VALUE);
      stream = new LowerCaseFilter(stream);
      stream = new StopFilter(true, stream, StopAnalyzer.ENGLISH_STOP_WORDS_SET);
      return stream;
   }

}
