// -------------------------------------------------------------------
//
// Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
//
// -------------------------------------------------------------------

package com.basho.search.analysis;

import java.io.IOException;

import org.apache.lucene.analysis.tokenattributes.TermAttribute;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;

public class IntegerPaddingFilter extends TokenFilter {
	  private TermAttribute termAtt;
	  private String fmt;

	  // Filter that zero-pads integers to the provided padding length 
	  public IntegerPaddingFilter(TokenStream in, int padLength) {
	    super(in);
	    termAtt = addAttribute(TermAttribute.class);
	    fmt = "%0"+padLength+"d";
	  }

	  @Override
	  public final boolean incrementToken() throws IOException {
	    if (!input.incrementToken())
	      return false;
	    String token = new String(termAtt.termBuffer(), 0, termAtt.termLength());
	    String padded = String.format(fmt, Integer.parseInt(token));
	    termAtt.setTermBuffer(padded);
	    return true;
	  }

}
