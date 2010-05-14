package com.basho.search.analysis.server;

import java.util.Iterator;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.net.InetAddress;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipelineCoverage;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.channel.ChannelState;
import org.jboss.netty.channel.ChannelStateEvent;

import com.basho.search.analysis.TextAnalyzer;
import com.basho.search.proto.Analysis.AnalysisError;
import com.basho.search.proto.Analysis.AnalysisRequest;
import com.basho.search.proto.Analysis.AnalysisResult;

@ChannelPipelineCoverage("all")
public class AnalysisHandler extends SimpleChannelUpstreamHandler {

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
