package com.basho.search.analysis.server;

import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;

public class AnalysisServer {

   public static void main(String[] argv) throws Exception {
      int port = Integer.parseInt(argv[0]);
      buildAnalysisServer(port);
      buildHeartbeatServer(port + 1);
   }
   
   private static void buildHeartbeatServer(int port) {
      ServerBootstrap bootstrap = new ServerBootstrap(
            new NioServerSocketChannelFactory(
                  Executors.newCachedThreadPool(),
                  Executors.newCachedThreadPool()));
      bootstrap.setOption("reuseAddress", true);
      bootstrap.setOption("child.tcpNoDelay", true);
      bootstrap.setPipelineFactory(new HeartbeatPipelineFactory());
      bootstrap.bind(new InetSocketAddress(port));
   }
   
   private static void buildAnalysisServer(int port) {
      ServerBootstrap bootstrap = new ServerBootstrap(
            new NioServerSocketChannelFactory(
                  Executors.newCachedThreadPool(),
                  Executors.newCachedThreadPool(), 
                  Runtime.getRuntime().availableProcessors() * 4));
      bootstrap.setOption("reuseAddress", true);
      bootstrap.setOption("child.tcpNoDelay", true);
      bootstrap.setPipelineFactory(new AnalysisPipelineFactory());
      bootstrap.bind(new InetSocketAddress(port));
   }
}
