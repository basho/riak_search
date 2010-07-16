// -------------------------------------------------------------------
//
// Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
//
// -------------------------------------------------------------------

package raptor.server;

import java.nio.ByteBuffer;

import org.apache.log4j.Logger;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipelineCoverage;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.json.JSONArray;
import org.json.JSONObject;

import raptor.protobuf.Messages.CatalogQuery;
import raptor.protobuf.Messages.CatalogQueryResponse;
import raptor.protobuf.Messages.Command;
import raptor.protobuf.Messages.CommandResponse;
import raptor.protobuf.Messages.DeleteEntry;
import raptor.protobuf.Messages.Index;
import raptor.protobuf.Messages.Info;
import raptor.protobuf.Messages.InfoRange;
import raptor.protobuf.Messages.InfoResponse;
import raptor.protobuf.Messages.MultiStream;
import raptor.protobuf.Messages.Stream;
import raptor.protobuf.Messages.StreamResponse;
import raptor.store.handlers.ResultHandler;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.MessageLite;

@ChannelPipelineCoverage("all")
public class RaptorHandler extends SimpleChannelUpstreamHandler {
    final private static Logger log =
            Logger.getLogger(RaptorHandler.class);

    /* see raptor_conn.erl for sender values */
    final private static short MSG_INDEX                   = 0;
    final private static short MSG_STREAM                  = 1;
    final private static short MSG_INFO                    = 2;
    final private static short MSG_INFORANGE               = 3;
    final private static short MSG_CATALOG_QUERY           = 4;
    final private static short MSG_MULTISTREAM             = 5;
    final private static short MSG_COMMAND                 = 6;
    final private static short MSG_DELETE_ENTRY            = 7;
    final private static short MSG_STREAM_RESPONSE         = 8;
    final private static short MSG_INFO_RESPONSE           = 9;
    final private static short MSG_CATALOG_QUERY_RESPONSE  = 10;
    final private static short MSG_COMMAND_RESPONSE        = 11;

    @Override
    public void channelConnected(ChannelHandlerContext ctx,
                                 ChannelStateEvent e) throws Exception {
        if (RaptorServer.shuttingDown) {
            ChannelFuture quitFuture = e.getChannel().write("");
            quitFuture.addListener(ChannelFutureListener.CLOSE);
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
        	short msgType = bbuf.getShort();
        	byte[] msgBytes = new byte[bbuf.remaining()];
        	bbuf.get(msgBytes);
        	
        	try {
	        	switch(msgType)
	        	{
	        	case MSG_INDEX:
	        		processIndexMessage(e, Index.parseFrom(msgBytes));
	        		break;
	        	case MSG_DELETE_ENTRY:
	        		processDeleteEntryMessage(DeleteEntry.parseFrom(msgBytes));
	        		break;
	        	case MSG_STREAM:
	        		processStreamMessage(e, Stream.parseFrom(msgBytes));
	        		break;
	        	case MSG_MULTISTREAM:
	        		processMultiStreamMessage(e, MultiStream.parseFrom(msgBytes));
	        		break;
	        	case MSG_INFO:
	        		processInfoMessage(e, Info.parseFrom(msgBytes));
	        		break;
	        	case MSG_INFORANGE:
	        		processInfoRangeMessage(e, InfoRange.parseFrom(msgBytes));
	        		break;
	        	case MSG_CATALOG_QUERY:
	        		processCatalogQueryMessage(e, CatalogQuery.parseFrom(msgBytes));
	        		break;
	        	case MSG_COMMAND:
	        		processCommandMessage(e, Command.parseFrom(msgBytes));
	        		break;
	        	default:
	        		log.error("Ignoring unknown message type: " + msgType);
	        	}
	        }
        	catch (InvalidProtocolBufferException ex) {
        		log.error("Error decoding message type: " + msgType, ex);
        	}
        }
    }
    
    private void sendMessage(Channel chan, short msgType, MessageLite msg)
    {
    	ChannelBuffer buffer = ChannelBuffers.dynamicBuffer();
    	buffer.writeShort(msgType);
    	buffer.writeBytes(msg.toByteArray());
    	chan.write(buffer);
    }

    private void processIndexMessage(MessageEvent e, Index msg) {
        try 
        {
        	if (msg.getIfnewer())
        	{
        		RaptorServer.idx.indexIfNewer(msg.getIndex(),
                        msg.getField(),
                        msg.getTerm(),
                        msg.getValue(),
                        msg.getPartition(),
                        msg.getProps().toByteArray(),
                        msg.getKeyClock());
        	}
        	else
        	{
        		RaptorServer.idx.index(msg.getIndex(),
                         msg.getField(),
                         msg.getTerm(),
                         msg.getValue(),
                         msg.getPartition(),
                         msg.getProps().toByteArray(),
                         msg.getKeyClock());
        	}
            CommandResponse.Builder response = CommandResponse.newBuilder();
            response.setResponse("");
            sendMessage(e.getChannel(), MSG_COMMAND_RESPONSE, response.build());
        } catch (Exception ex) {
            log.error("Error handling index request", ex);
        }
    }

    private void processDeleteEntryMessage(DeleteEntry msg) {
        try {
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
    	if (RaptorServer.debugging) log.info("stream: " + msg);
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
                                response.setValue(new String(key, "UTF-8"))
                                        .setProps(ByteString.copyFrom(value))
                                        .setKeyClock(ByteString.copyFrom(key_clock));
                                sendMessage(chan, MSG_STREAM_RESPONSE, response.build());
                            } catch (Exception ex) {
                                ex.printStackTrace();
                            }
                        }

                        public void handleResult(String key, String value,
                                                 String key_clock) {
                            try {
                                StreamResponse.Builder response =
                                        StreamResponse.newBuilder();
                                response.setValue(key)
                                        .setProps(ByteString.copyFrom(
                                                value.getBytes("UTF-8")))
                                        .setKeyClock(ByteString.copyFrom(
                                                key_clock.getBytes("UTF-8")));
                                sendMessage(chan, MSG_STREAM_RESPONSE, response.build());
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
        if (RaptorServer.debugging) log.info("multistream: " + msg);
        try {
            final Channel chan = e.getChannel();
            String[] iftStrings = msg.getTermList().split("\\`");
            JSONArray terms = new JSONArray();
            for (String iftStr : iftStrings) {
                String[] ift = iftStr.split("~");
                JSONObject jo = new JSONObject();
                jo.put("index", ift[0]);
                jo.put("field", ift[1]);
                jo.put("term", ift[2]);
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
                                sendMessage(chan, MSG_STREAM_RESPONSE, response.build());
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
                            String term = parts[parts.length - 1];
                            InfoResponse.Builder response =
                                    InfoResponse.newBuilder();
                            response.setTerm(term)
                                    .setCount(count);
                            sendMessage(chan, MSG_INFO_RESPONSE, response.build());
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
                            sendMessage(chan, MSG_INFO_RESPONSE, response.build());
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
                                sendMessage(chan, MSG_CATALOG_QUERY_RESPONSE, response.build());
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
                            sendMessage(chan, MSG_INFO_RESPONSE, response.build());
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

                sendMessage(chan, MSG_COMMAND_RESPONSE, response.build());
                RaptorServer.shuttingDown = true;
                RaptorServer.idx.sync();
                System.exit(0);

            } else if (cmd.equals("status")) {
                Runtime runtime = Runtime.getRuntime();
                String s = "freeMemory = " + (runtime.freeMemory() / 1024) + "kB`" +
                           "maxMemory (config) = " + (runtime.maxMemory() / 1024) + " kB`" +
                           "system properties: " + System.getProperties() + "`" +
                           "system environment: " + System.getenv() + "`";
                response.setResponse(s);

            } else if (cmd.equals("freememory")) {
                response.setResponse("" + ((Runtime.getRuntime()).freeMemory() / 1024));

            } else {
                response.setResponse("Unknown command: " + cmd);
            }

            sendMessage(chan, MSG_COMMAND_RESPONSE, response.build());
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}

