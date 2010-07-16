// -------------------------------------------------------------------
//
// Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
//
// -------------------------------------------------------------------

package raptor.server;

import org.jboss.netty.channel.*;

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
