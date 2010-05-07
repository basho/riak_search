package com.basho.search.analysis.server;

import java.util.Iterator;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipelineCoverage;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;

import com.basho.search.analysis.TextAnalyzer;
import com.basho.search.proto.Analysis.AnalysisError;
import com.basho.search.proto.Analysis.AnalysisRequest;
import com.basho.search.proto.Analysis.AnalysisResult;

@ChannelPipelineCoverage("all")
public class AnalysisHandler extends SimpleChannelUpstreamHandler {

   private static final Logger logger = Logger.getLogger(AnalysisHandler.class.getName());

   public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) {
      AnalysisRequest request = (AnalysisRequest) e.getMessage();
      if (request.getText().equals("__basho_analyzer_monitor_stop__")) {
         System.exit(0);
      }
      else {
         doAnalyze(request, e);
      }
   }
   
   private void doAnalyze(AnalysisRequest request, MessageEvent e) {
      String text = request.getText();
      Channel chan = e.getChannel();
      try {
         List<String> tokens = TextAnalyzer.analyze(text);
         if (tokens.size() > 0) {
            for(Iterator<String> i = tokens.iterator(); i.hasNext();) {
               AnalysisResult.Builder builder = AnalysisResult.newBuilder();
               builder.setToken(i.next());
               builder.setDone(i.hasNext() ? 0 : 1);
               chan.write(builder.build());
            }
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
