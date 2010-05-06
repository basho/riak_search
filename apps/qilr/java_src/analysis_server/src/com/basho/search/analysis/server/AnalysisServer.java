package com.basho.search.analysis.server;

import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;

public class AnalysisServer {

   public static void main(String[] argv) throws Exception {
      ServerBootstrap bootstrap = new ServerBootstrap(
            new NioServerSocketChannelFactory(
                  Executors.newCachedThreadPool(),
                  Executors.newCachedThreadPool()));
      bootstrap.setPipelineFactory(new AnalysisPipelineFactory());
      bootstrap.bind(new InetSocketAddress(6098));
   }
}
