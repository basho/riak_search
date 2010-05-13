package com.basho.search.analysis.server;

import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;

   class HeartbeatPipelineFactory implements ChannelPipelineFactory {

      public ChannelPipeline getPipeline() throws Exception {
         ChannelPipeline p = Channels.pipeline();
         p.addLast("handler", new HeartbeatHandler());
         return p;
      }
   }