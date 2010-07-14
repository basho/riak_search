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

import java.nio.ByteBuffer;

import org.apache.log4j.Logger;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelEvent;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipelineCoverage;
import org.jboss.netty.channel.ChannelState;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.json.JSONObject;
import org.json.JSONArray;

import raptor.protobuf.Messages.CatalogQuery;
import raptor.protobuf.Messages.CatalogQueryResponse;
import raptor.protobuf.Messages.Command;
import raptor.protobuf.Messages.CommandResponse;
import raptor.protobuf.Messages.DeleteEntry;
import raptor.protobuf.Messages.Index;
import raptor.protobuf.Messages.Info;
import raptor.protobuf.Messages.InfoRange;
import raptor.protobuf.Messages.InfoResponse;
import raptor.protobuf.Messages.Stream;
import raptor.protobuf.Messages.MultiStream;
import raptor.protobuf.Messages.StreamResponse;
import raptor.store.handlers.ResultHandler;

import com.google.protobuf.ByteString;

@ChannelPipelineCoverage("all")
public class RaptorHandler extends SimpleChannelUpstreamHandler {
   final private static Logger log = 
      Logger.getLogger(RaptorHandler.class);
   final private static String MSG_INFO = "Info";
   final private static String MSG_INDEX = "Index";
   final private static String MSG_INDEX_IF_NEWER = "IndexIfNewer";
   final private static String MSG_DELETE_ENTRY = "DeleteEntry";
   final private static String MSG_STREAM = "Stream";
   final private static String MSG_MULTISTREAM = "MultiStream";
   final private static String MSG_INFORANGE = "InfoRange";
   final private static String MSG_CATALOG_QUERY = "CatalogQuery";
   final private static String MSG_COMMAND = "Command";

//    @Override
//    public void handleUpstream(
//            ChannelHandlerContext ctx, ChannelEvent e) throws Exception {
//        if (e instanceof ChannelStateEvent) {
//            ChannelState state = ( (ChannelStateEvent) e).getState();
//            if (state == state.CONNECTED &&
//                ( (ChannelStateEvent)e).getValue() == null) {
//                //log.info("> DISCONNECTED");
//            } else {
//                //log.info(e.toString());
//            }
//        }
//        super.handleUpstream(ctx, e);
//    }

    @Override
    public void channelConnected(ChannelHandlerContext ctx, 
                                 ChannelStateEvent e) throws Exception {
            if (RaptorServer.shuttingDown) {
                ChannelFuture quitFuture = e.getChannel().write("");
                quitFuture.addListener(ChannelFutureListener.CLOSE);
                return;
            } else {
                if (RaptorServer.debugging) log.info(e.toString());
            }
    }

   public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) {
      if (RaptorServer.shuttingDown) {
         ChannelFuture quitFuture = e.getChannel().write("");
         quitFuture.addListener(ChannelFutureListener.CLOSE);
         return;
      }
      ByteBuffer bbuf = ((ChannelBuffer) e.getMessage()).toByteBuffer();
      if (bbuf.hasArray()) {
         byte[] b_ar = bbuf.array();
         
         // index
         try {
            Index index = Index.newBuilder().mergeFrom(b_ar).build();
            if (index.getMessageType().equals(MSG_INDEX)) {
               processIndexMessage(e, index);
               return;
            }
            else if (index.getMessageType().equals(MSG_INDEX_IF_NEWER)) {
               processIndexIfNewerMessage(e, index);
               return;
            }
         } catch (com.google.protobuf.UninitializedMessageException ex) { }
           catch (com.google.protobuf.InvalidProtocolBufferException ex) { }
         
         // delete
         try {
            DeleteEntry deleteEntry = DeleteEntry.newBuilder().mergeFrom(b_ar).build();
            if (deleteEntry.getMessageType().equals(MSG_DELETE_ENTRY)) {
                processDeleteEntryMessage(deleteEntry);
                return;
            }
         } catch (com.google.protobuf.UninitializedMessageException ex) { }
           catch (com.google.protobuf.InvalidProtocolBufferException ex) { }

         // stream
         try {
            Stream stream = Stream.newBuilder().mergeFrom(b_ar).build();
            if (stream.getMessageType().equals(MSG_STREAM)) {
               if (RaptorServer.debugging) log.info("stream: " + stream);
               processStreamMessage(e, stream);
               return;
            }
         } catch (com.google.protobuf.UninitializedMessageException ex) { }
           catch (com.google.protobuf.InvalidProtocolBufferException ex) { }
         
         // multi_stream
         try {
            MultiStream multistream = MultiStream.newBuilder().mergeFrom(b_ar).build();
            if (multistream.getMessageType().equals(MSG_MULTISTREAM)) {
               if (RaptorServer.debugging) log.info("multistream: " + multistream);
               processMultiStreamMessage(e, multistream);
               return;
            }
         } catch (com.google.protobuf.UninitializedMessageException ex) { }
           catch (com.google.protobuf.InvalidProtocolBufferException ex) { }
         
         // info
         try {
            Info info = Info.newBuilder().mergeFrom(b_ar).build();
            if (info.getMessageType().equals(MSG_INFO)) {
               if (RaptorServer.debugging) log.info("info = " + info);
               processInfoMessage(e, info);
               return;
            }
         } catch (com.google.protobuf.UninitializedMessageException ex) { }
           catch (com.google.protobuf.InvalidProtocolBufferException ex) { }
         
         // info_range
         try {
            InfoRange infoRange = InfoRange.newBuilder().mergeFrom(b_ar).build();
            if (infoRange.getMessageType().equals(MSG_INFORANGE)) {
               if (RaptorServer.debugging) log.info("infoRange = " + infoRange);
               processInfoRangeMessage(e, infoRange);
               return;
            }
         } catch (com.google.protobuf.UninitializedMessageException ex) { }
           catch (com.google.protobuf.InvalidProtocolBufferException ex) { }
           
         // catalog_query
         try {
            CatalogQuery catalogQuery = 
                CatalogQuery.newBuilder().mergeFrom(b_ar).build();
            if (catalogQuery.getMessageType().equals(MSG_CATALOG_QUERY)) {
               if (RaptorServer.debugging) log.info("catalogQuery: " + catalogQuery);
               processCatalogQueryMessage(e, catalogQuery);
               return;
            }
         } catch (com.google.protobuf.UninitializedMessageException ex) { }
           catch (com.google.protobuf.InvalidProtocolBufferException ex) { }

         // command ("sys-ex")
         try {
            Command command = 
                Command.newBuilder().mergeFrom(b_ar).build();
            if (command.getMessageType().equals(MSG_COMMAND)) {
               log.info("command: " + command);
               processCommandMessage(e, command);
               return;
            }
         } catch (com.google.protobuf.UninitializedMessageException ex) { }
           catch (com.google.protobuf.InvalidProtocolBufferException ex) { }

         try {
            log.info("unknown message: b_ar = " + new String(b_ar, "UTF-8"));
         } catch (Exception ex) { ex.printStackTrace(); }
         
      } else {
         log.info("bbuf.hasArray() -> false?  bbuf = " + bbuf);
      }
   }

   private void processIndexMessage(MessageEvent e, Index msg) {
      try {
         CommandResponse.Builder response =
            CommandResponse.newBuilder();
         final Channel chan = e.getChannel();
         
         //RaptorServer.writeQueue.put(msg);
         RaptorServer.idx.index(msg.getIndex(),
             msg.getField(),
             msg.getTerm(),
             msg.getValue(),
             msg.getPartition(),
             msg.getProps().toByteArray(),
             msg.getKeyClock());
         
         response.setResponse("");
         CommandResponse r = response.build();
         chan.write(r);
      } catch (Exception ex) {
         log.error("Error handling index request", ex);
      }
   }

   private void processIndexIfNewerMessage(MessageEvent e, Index msg) {
      try {
         CommandResponse.Builder response =
            CommandResponse.newBuilder();
         final Channel chan = e.getChannel();
         
         //RaptorServer.writeQueue.put(msg);
         RaptorServer.idx.index_if_newer(msg.getIndex(),
             msg.getField(),
             msg.getTerm(),
             msg.getValue(),
             msg.getPartition(),
             msg.getProps().toByteArray(),
             msg.getKeyClock());
         
         response.setResponse("");
         CommandResponse r = response.build();
         chan.write(r);
      } catch (Exception ex) {
         log.error("Error handling index if newer request", ex);
      }
   }
   
   private void processDeleteEntryMessage(DeleteEntry msg) {
      try {
         //RaptorServer.writeQueue.put(msg);
         RaptorServer.idx.deleteEntry(msg.getIndex(),
                            msg.getField(),
                            msg.getTerm(),
                            msg.getDocId(),
                            msg.getPartition());
      } catch (Exception ex) {
         log.error("Error handling delete", ex);
      }
   }
   
   private void processStreamMessage(MessageEvent e, Stream msg) {
      try {
         final Channel chan = e.getChannel();
         RaptorServer.idx.stream(msg.getIndex(),
                                 msg.getField(),
                                 msg.getTerm(),
                                 msg.getPartition(),
                                 new ResultHandler() {
                                        public void handleResult(byte[] key, byte[] value,
                                        			byte[] key_clock) {
                                            try {
                                               StreamResponse.Builder response = 
                                                   StreamResponse.newBuilder();
                                               if (value == null) {
                                                  throw new Exception("processStreamMessage: value == null for key '" +
                                                    new String(key, "UTF-8"));
                                               }
                                               response.setValue(new String(key, "UTF-8"))
                                                       .setProps(ByteString.copyFrom(value))
                                                       .setKeyClock(ByteString.copyFrom(key_clock));
                                               chan.write(response.build());
                                            } catch (Exception ex) {
                                               ex.printStackTrace();
                                            }
                                        }
                                        public void handleResult(String key, String value,
                                        			             String key_clock) {
                                            try {
                                                StreamResponse.Builder response = 
                                                    StreamResponse.newBuilder();
                                                if (value == null) {
                                                  throw new Exception("processStreamMessage: value == null for key '" +
                                                    key);
                                                }
                                                response.setValue(key)
                                                        .setProps(ByteString.copyFrom(
                                                            value.getBytes("UTF-8")))
                                                        .setKeyClock(ByteString.copyFrom(
                                                        	key_clock.getBytes("UTF-8")));
                                                chan.write(response.build());
                                            } catch (Exception ex) {
                                               ex.printStackTrace();
                                            }
                                        }
                                 });
      } catch (Exception ex) {
         log.error("Error handling stream query", ex);
      }
   }
   
   private void processMultiStreamMessage(MessageEvent e, MultiStream msg) {
      try {
         final Channel chan = e.getChannel();
         String[] iftStrings = msg.getTermList().split("\\`");
         JSONArray terms = new JSONArray();
         for(String iftStr: iftStrings) {
            String[] ift = iftStr.split("~");
            JSONObject jo = new JSONObject();
            jo.put("index", ift[0]);
            jo.put("field", ift[1]);
            jo.put("term", ift[2]);
            
            //log.info("processMultiStreamMessage: jo = " + jo.toString(4));
                
            terms.put(jo);
         }
         
         RaptorServer.idx.multistream(terms,
                                      new ResultHandler() {
                                        public void handleResult(byte[] key, byte[] value) {
                                            try {
                                               StreamResponse.Builder response =
                                                   StreamResponse.newBuilder();
                                               response.setValue(new String(key, "UTF-8"))
                                                       .setProps(ByteString.copyFrom(value));
                                               chan.write(response.build());
                                            } catch (Exception ex) {
                                               ex.printStackTrace();
                                            }
                                        }
                                        public void handleResult(String key, String value) {
                                            try {
                                                StreamResponse.Builder response =
                                                    StreamResponse.newBuilder();
                                                response.setValue(key)
                                                        .setProps(ByteString.copyFrom(
                                                            value.getBytes("UTF-8")));
                                                chan.write(response.build());
                                            } catch (Exception ex) {
                                               ex.printStackTrace();
                                            }
                                        }
                                   });
      } catch (Exception ex) {
         ex.printStackTrace();
      }
   }

   private void processInfoMessage(MessageEvent e, Info msg) {
      try {
         final Channel chan = e.getChannel();
         RaptorServer.idx.info(msg.getIndex(),
                               msg.getField(),
                               msg.getTerm(),
                               msg.getPartition(),
                                new ResultHandler() {
                                  public void handleInfoResult(String bucket, long count) {
                                      String[] parts = bucket.split("\\/");
                                      String term = parts[parts.length-1];
                                      InfoResponse.Builder response = 
                                          InfoResponse.newBuilder();
                                      response.setTerm(term)
                                              .setCount(count);
                                      chan.write(response.build());
                                      
                                  }
                                });
      } catch (Exception ex) {
         log.error("Error handling info query", ex);
      }
   }
   
   private void processInfoRangeMessage(MessageEvent e, InfoRange msg) {
      try {
         final Channel chan = e.getChannel();
         RaptorServer.idx.infoRange(msg.getIndex(),
                                    msg.getField(),
                                    msg.getStartTerm(),
                                    msg.getEndTerm(),
                                    msg.getPartition(),
                                      new ResultHandler() {
                                        public void handleInfoResult(String bucket, 
                                          long count) {
                                            InfoResponse.Builder response = 
                                                InfoResponse.newBuilder();
                                            response.setTerm(bucket)
                                                    .setCount(count);
                                            chan.write(response.build());
                                        }
                                      });
      } catch (Exception ex) {
         log.error("Error handling info range query", ex);
      }
   }
   
   private void processCatalogQueryMessage(MessageEvent e, CatalogQuery msg) {
      try {
         final Channel chan = e.getChannel();
         RaptorServer.idx.catalogQuery(msg.getSearchQuery(),
                                       msg.getMaxResults(),
                                      new ResultHandler() {
                                        public void handleCatalogResult(JSONObject obj) {
                                            try {
                                                CatalogQueryResponse.Builder response =
                                                    CatalogQueryResponse.newBuilder();
                                                response.setPartition(obj.getString("partition_id"))
                                                        .setIndex(obj.getString("index"))
                                                        .setField(obj.getString("field"))
                                                        .setTerm(obj.getString("term"));
                                                obj.remove("partition_id");
                                                obj.remove("index");
                                                obj.remove("field");
                                                obj.remove("term");
                                                response.setJsonProps(obj.toString());
                                                chan.write(response.build());
                                            } catch (org.json.JSONException ex) {
                                                ex.printStackTrace();
                                            }
                                        }
                                        
                                        // $end_of_results
                                        public void handleInfoResult(String bucket, long count) {
                                            InfoResponse.Builder response = 
                                                InfoResponse.newBuilder();
                                            response.setTerm(bucket)
                                                    .setCount(count);
                                            chan.write(response.build());
                                        }
                                      });
      } catch (Exception ex) {
         log.error("Error handling catalog query", ex);
      }
   }
   
   private void processCommandMessage(MessageEvent e, Command msg) {
    try {
        CommandResponse.Builder response =
            CommandResponse.newBuilder();
        final Channel chan = e.getChannel();
        String cmd = msg.getCommand();

        if (cmd.equals("sync")) {
            RaptorServer.idx.sync();
            response.setResponse("ok");

        } else if (cmd.equals("drop_partition")) {
            RaptorServer.idx.dropPartition(msg.getArg1());
            response.setResponse("partition_dropped");

        } else if (cmd.equals("partition_count")) {
            response.setResponse("" +
                RaptorServer.idx.partitionCount(msg.getArg1()));
        
        } else if (cmd.equals("get_entry_keyclock")) {
            String iftStr = msg.getArg1();
            String partition = msg.getArg2();
            String docId = msg.getArg3();
            String[] ift = iftStr.split("~");
            String index = ift[0];
            String field = ift[1];
            String term = ift[2];
            String keyClock = 
                RaptorServer.idx.getEntryKeyClock(index,
                                                  field,
                                                  term,
                                                  docId,
                                                  partition);
            response.setResponse(keyClock);
        
        } else if (cmd.equals("toggle_debug")) {
            if (RaptorServer.debugging) {
                RaptorServer.debugging = false;
                response.setResponse("off");
            } else {
                RaptorServer.debugging = true;
                response.setResponse("on");
            }
            
        } else if (cmd.equals("shutdown")) {
            log.info("shutdown issued by riak_search");
            response.setResponse("shutting_down");
            
            /* ship response now & boot */
            CommandResponse r = response.build();
            log.info("CommandResponse = " + r);
            chan.write(r);
            
            RaptorServer.shuttingDown = true;
            RaptorServer.idx.sync();
            System.exit(0);

        } else if (cmd.equals("status")) {
            Runtime runtime = Runtime.getRuntime();
            String s = "writeQueue size = " + RaptorServer.writeQueue.size() + "`" +
                       "freeMemory = " + (runtime.freeMemory()/1024) + "kB`" +
                       "maxMemory (config) = " + (runtime.maxMemory()/1024) + " kB`" + 
                       "system properties: " + System.getProperties() + "`" +
                       "system environment: " + System.getenv() + "`";
            response.setResponse(s);
        
        } else if (cmd.equals("queuesize")) {
            response.setResponse("" + RaptorServer.writeQueue.size());
            
        } else if (cmd.equals("freememory")) {
            response.setResponse("" + ((Runtime.getRuntime()).freeMemory()/1024));
        
        } else {
            response.setResponse("Unknown command: " + cmd);
        }
        
        CommandResponse r = response.build();
        log.info("CommandResponse = " + r);
        chan.write(r);
    } catch (Exception ex) {
        ex.printStackTrace();
    }
   }
   
   public void exceptionCaught(ChannelHandlerContext ctx,
                               ExceptionEvent e) throws Exception {
      Throwable ex = e.getCause();
      if (ex instanceof java.io.IOException) return;
      log.info("exceptionCaught: " + ex.toString());
      log.info("ExceptionEvent e = " + e.toString());
      ex.printStackTrace();
      log.info("----");
   }
}

