// -------------------------------------------------------------------
//
// Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
//
// -------------------------------------------------------------------

package com.basho.search.analysis;

import java.io.Reader;
import org.apache.lucene.analysis.CharTokenizer;
import org.apache.lucene.util.AttributeSource;

public class IntegerTokenizer extends CharTokenizer {
    public IntegerTokenizer(Reader in) {
        super(in);
    }

    /** Construct a new WhitespaceTokenizer using a given {@link AttributeSource}. */
    public IntegerTokenizer(AttributeSource source, Reader in) {
        super(source, in);
    }

    /** Construct a new WhitespaceTokenizer using a given {@link org.apache.lucene.util.AttributeSource.AttributeFactory}. */
    public IntegerTokenizer(AttributeFactory factory, Reader in) {
        super(factory, in);
    }
      
    /** Collects only characters which do not satisfy
     * {@link Character#isWhitespace(char)}.*/
    @Override
    protected boolean isTokenChar(char c) {
        return (Character.isDigit(c) || c == '-');
    }
}
