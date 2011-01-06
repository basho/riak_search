package com.basho.search.analysis;

import java.io.IOException;
import java.io.StringReader;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.TermAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.util.Version;

public class TextAnalyzer {

	private static final Logger logger = Logger.getLogger(TextAnalyzer.class.getName());

	public static List<String> analyze(String text, String analyzerFactory,
			String[] args) throws IOException {
		// Setup token stream and filters

		AnalyzerFactory factory = instantiateFactory(analyzerFactory);
		TokenStream stream = factory.makeStream(Version.LUCENE_30, new StringReader(text),
				args);

		// Prepare to iterate and collect tokens
		List<String> retval = new LinkedList<String>();
		stream.reset();
		do {
			if (stream.hasAttribute(PositionIncrementAttribute.class)) {
				PositionIncrementAttribute pia = stream.getAttribute(PositionIncrementAttribute.class);
				for (int i = pia.getPositionIncrement(); i > 1; i--) {
					retval.add(""); // insert filtered word placeholders
				}
			} 

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

	@SuppressWarnings("unchecked")
	private static AnalyzerFactory instantiateFactory(String factoryClassName) {
		AnalyzerFactory retval = null;
		try {
			Class<AnalyzerFactory> klass = (Class<AnalyzerFactory>) Class.forName(factoryClassName);
			retval = klass.newInstance();
		}
		catch (Exception e) {
			logger.log(Level.WARNING, "Failure creating instance of " + factoryClassName, e);
			logger.log(Level.WARNING, "Failure to create AnalyzerFactory: " + factoryClassName + "." +
			" Falling back to whitespace analyzer factory");
			return new WhitespaceAnalyzerFactory();
		}

		return retval;
	}
}
