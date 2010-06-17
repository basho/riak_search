package com.basho.search.analysis.server;

import java.io.FileOutputStream;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipelineCoverage;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;

import com.basho.search.analysis.DefaultAnalyzerFactory;
import com.basho.search.analysis.TextAnalyzer;
import com.basho.search.proto.Analysis.AnalysisError;
import com.basho.search.proto.Analysis.AnalysisRequest;
import com.basho.search.proto.Analysis.AnalysisResult;

@ChannelPipelineCoverage("all")
public class AnalysisHandler extends SimpleChannelUpstreamHandler {

   private static final String DEFAULT_ANALYZER_FACTORY = DefaultAnalyzerFactory.class.getName();
   
   private static final Logger logger = Logger.getLogger(AnalysisHandler.class.getName());

   @Override
   public void channelConnected(ChannelHandlerContext ctx, 
                                ChannelStateEvent e) throws Exception {
      // this would be nice, but it makes qilr_analyzer_monitor crash. :/
      /*
      logger.info(e.toString() + ": channelConnected: " + 
         InetAddress.getLocalHost().getHostName());
      */
   }
   
   public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) {
      AnalysisRequest request = (AnalysisRequest) e.getMessage();
      doAnalyze(request, e);
   }
   
   private void doAnalyze(AnalysisRequest request, MessageEvent e) {
      String text = request.getText();
      String analyzerFactory = DEFAULT_ANALYZER_FACTORY;
      if (request.hasAnalyzerFactory()) {
         analyzerFactory = request.getAnalyzerFactory();
      }
      try {
         String message = "Calling " + analyzerFactory + "\n";
         FileOutputStream fout = new FileOutputStream("/tmp/analyzer_out.txt", true);
         fout.write(message.getBytes());
         message = "Client sent " + request.getAnalyzerFactory() + "\n";
         fout.write(message.getBytes());
         fout.close();
      }
      catch (Exception ex) {}

      Channel chan = e.getChannel();
      try {
         List<String> tokens = TextAnalyzer.analyze(text, analyzerFactory);
         if (tokens.size() > 0) {
            StringBuilder buf = new StringBuilder();
            if (tokens.size() == 1) {
               buf.append(tokens.get(0)).append("\0");
            }
            else {
               for(Iterator<String> i = tokens.iterator(); i.hasNext();) {
                  buf.append(i.next());
                  if(i.hasNext()) {
                     buf.append("\0");
                  }
               }
            }
            AnalysisResult.Builder builder = AnalysisResult.newBuilder();
            builder.setToken(buf.toString());
            builder.setDone(1);
            chan.write(builder.build());
         }
         else {
            AnalysisResult.Builder builder = AnalysisResult.newBuilder();
            builder.setToken("");
            builder.setDone(1);
            chan.write(builder.build());
         }
      }
      catch (Exception ex) {
         logger.log(Level.SEVERE, ex.getMessage(), ex);
         AnalysisError.Builder builder = AnalysisError.newBuilder();
         builder.setDescription(ex.getMessage());
         chan.write(builder.build());
      }      
   }
}
