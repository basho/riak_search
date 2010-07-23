// -------------------------------------------------------------------
//
// Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
//
// -------------------------------------------------------------------

package com.basho.search.analysis;

import java.io.StringReader;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.util.Version;

public class IntegerAnalyzerFactory implements AnalyzerFactory {

	public TokenStream makeStream(Version version, StringReader input,
				                  String[] args) {
			  int padLen = 10;
			  if (args.length == 1)
			  {
				  padLen = Integer.parseInt(args[0]);
			  }
		      TokenStream stream = new IntegerTokenizer(input);
		      stream = new IntegerPaddingFilter(stream, padLen);
		      return stream;
		   }
}
