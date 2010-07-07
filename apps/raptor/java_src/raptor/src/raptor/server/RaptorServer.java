/*
%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------
%%
%% J. Muellerleile
%%
*/

package raptor.server;

import java.net.InetSocketAddress;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.log4j.Logger;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;

import raptor.protobuf.Messages.Index;
import raptor.store.RSXIndex;

public class RaptorServer {
    final private static Logger log = 
        Logger.getLogger(RaptorServer.class);
    
    public static RSXIndex idx;
    public static Thread idxThread;
    
    public static Thread writeThread;
    public static LinkedBlockingQueue<Index> writeQueue;
    
    public static boolean shuttingDown = false;
    public static boolean debugging = true;
    
    static {        
        Runtime.getRuntime().addShutdownHook(
            new Thread() {
                public void run() {
                    RaptorServer.shuttingDown = true;
                    log.info("shutting down... [" + 
                        RaptorServer.writeQueue.size() + " queued]");
                    while(RaptorServer.writeQueue.size() > 0) {
                        log.info("draining writeQueue (" +
                            RaptorServer.writeQueue.size() + ")");
                        try {
                            Thread.sleep(1000);
                        } catch (Exception ex) { ex.printStackTrace(); }
                    }
                    try {
                        log.info("sync, close, shutdown ...");
                        RaptorServer.idx.shutdown();
                        log.info("closing...");
                    } catch (Exception ex) {
                        ex.printStackTrace();
                        log.info("Problem sync/close [crash] - verify and restore recommended");
                    }
                }
            });
    }
    
    public static void main(String[] argv) throws Exception {
        log.info("starting");
        int raptorPort = Integer.parseInt(argv[0]);
        String dataDir = argv[1];
        configureStorage(dataDir);
        buildRaptorServer(raptorPort);
        buildHeartbeatServer(raptorPort + 1);
    }
    
    private static void configureStorage(String dataDir) {
       // TODO: configurable writeQueue size?
       writeQueue = new LinkedBlockingQueue<Index>(5000);
       writeThread = new Thread(new Runnable() {
           public void run() {
               try {
                   log.info("writeThread: started");
                   while(true) {
                       try {
                           Index msg = writeQueue.take();
                           idx.index(msg.getIndex(),
                                     msg.getField(),
                                     msg.getTerm(),
                                     msg.getValue(),
                                     msg.getPartition(),
                                     msg.getProps().toByteArray());
                       } catch (Exception iex) {
                           iex.printStackTrace();
                       }
                   }
               } catch (Exception ex) {
                   ex.printStackTrace();
               }
           }
       });
       
       try {
           idx = new RSXIndex(dataDir);
           idxThread = new Thread(idx);
           idxThread.start();
       } catch (Exception ex) {
           idx = null;
           log.error("Error configuring Raptor storage", ex);
           System.exit(-1);
       }
    }
    
    private static void buildRaptorServer(int port) {
       ServerBootstrap bootstrap = new ServerBootstrap(
             new NioServerSocketChannelFactory(
                   Executors.newCachedThreadPool(),
                   Executors.newCachedThreadPool(),
                   Runtime.getRuntime().availableProcessors()));
         bootstrap.setOption("reuseAddress", true);
         bootstrap.setOption("child.tcpNoDelay", false);
         bootstrap.setPipelineFactory(new RaptorPipelineFactory());
         bootstrap.bind(new InetSocketAddress(port));
    }
    
    private static void buildHeartbeatServer(int port) {
       ServerBootstrap bootstrap = new ServerBootstrap(
             new NioServerSocketChannelFactory(
                   Executors.newCachedThreadPool(),
                   Executors.newCachedThreadPool()));
         bootstrap.setOption("reuseAddress", true);
         bootstrap.setOption("child.tcpNoDelay", false);
         bootstrap.setPipelineFactory(new HeartbeatPipelineFactory());
         bootstrap.bind(new InetSocketAddress(port));
    }    
}

