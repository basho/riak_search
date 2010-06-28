package com.basho.search.analysis;

import java.io.StringReader;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.WhitespaceTokenizer;
import org.apache.lucene.util.Version;

public class WhitespaceAnalyzerFactory implements AnalyzerFactory {

   public TokenStream makeStream(Version version, StringReader input) {
      return new WhitespaceTokenizer(input);
   }

}
