package com.basho.search.analysis.server;

import java.util.logging.Logger;

import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipelineCoverage;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ChildChannelStateEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;

@ChannelPipelineCoverage("all")
public class HeartbeatHandler extends SimpleChannelUpstreamHandler {

   private static final Logger logger = Logger.getLogger(AnalysisHandler.class.getName());

   public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e) {
      HeartbeatThread.getHeartbeatThread().stopCountdown();
   }
   
   public void channelDisconnected(ChannelHandlerContext ctx, ChannelStateEvent e) {
      HeartbeatThread.getHeartbeatThread();
   }
   
   public void childChannelClosed(ChannelHandlerContext ctx, ChildChannelStateEvent e) { 
      HeartbeatThread.getHeartbeatThread();
   }
}
