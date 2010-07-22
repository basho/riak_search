// -------------------------------------------------------------------
//
// Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
//
// -------------------------------------------------------------------

package com.basho.search.analysis;

import java.io.StringReader;

import org.apache.lucene.analysis.LengthFilter;
import org.apache.lucene.analysis.LowerCaseFilter;
import org.apache.lucene.analysis.StopAnalyzer;
import org.apache.lucene.analysis.StopFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.WhitespaceTokenizer;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.util.Version;

public class IntegerAnalyzerFactory implements AnalyzerFactory {

	public TokenStream makeStream(Version version, StringReader input) {
		      TokenStream stream = new IntegerTokenizer(input);
		      stream = new IntegerPaddingFilter(stream, 10);
		      return stream;
		   }
}
