package raptor.server;

import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipelineCoverage;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ChildChannelStateEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;

@ChannelPipelineCoverage("all")
public class HeartbeatHandler extends SimpleChannelUpstreamHandler {

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
