package com.basho.search.analysis;

import java.io.IOException;
import java.io.StringReader;
import java.util.LinkedList;
import java.util.List;

import org.apache.lucene.analysis.LengthFilter;
import org.apache.lucene.analysis.LowerCaseFilter;
import org.apache.lucene.analysis.StopAnalyzer;
import org.apache.lucene.analysis.StopFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.WhitespaceTokenizer;
import org.apache.lucene.analysis.tokenattributes.TermAttribute;

public class TextAnalyzer {

   public static List<String> analyze(String text) throws IOException {
      // Setup token stream and filters
      TokenStream stream = new WhitespaceTokenizer(new StringReader(text));
      stream = new LengthFilter(stream, 3, Integer.MAX_VALUE);
      stream = new LowerCaseFilter(stream);
      stream = new StopFilter(false, stream, StopAnalyzer.ENGLISH_STOP_WORDS_SET);
      // Prepare to iterate and collect tokens
      List<String> retval = new LinkedList<String>();
      stream.reset();
      do {
         TermAttribute ta = stream.getAttribute(TermAttribute.class);
         if (ta.termLength() > 0) {
            retval.add(ta.term());
         }
      } while (stream.incrementToken());
      // Clean up
      stream.end();
      stream.close();
      return retval;
   }
}
