package com.basho.search.analysis.server;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipelineCoverage;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;

import com.basho.search.analysis.DefaultAnalyzerFactory;
import com.basho.search.analysis.TextAnalyzer;
import com.basho.search.proto.Analysis.AnalysisError;
import com.basho.search.proto.Analysis.AnalysisRequest;
import com.basho.search.proto.Analysis.AnalysisResult;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.MessageLite;

@ChannelPipelineCoverage("all")
public class AnalysisHandler extends SimpleChannelUpstreamHandler {

    private static final String DEFAULT_ANALYZER_FACTORY = DefaultAnalyzerFactory.class.getName();
    /* see qilr_analyzer.erl for sender values */
    final private static short MSG_ANALYZE                   = 1;
    final private static short MSG_ANALYSIS_RESULT           = 2;
    final private static short MSG_ANALYSIS_ERROR            = 3;
   
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
        ByteBuffer bbuf = ((ChannelBuffer) e.getMessage()).toByteBuffer();
        if (bbuf.hasArray()) {
            short msgType = bbuf.getShort();
            byte[] msgBytes = new byte[bbuf.remaining()];
            bbuf.get(msgBytes);
        
            try {
                switch(msgType) {
                case MSG_ANALYZE:
                    doAnalyze(e, AnalysisRequest.parseFrom(msgBytes));
                    break;
                }
            }
            catch (InvalidProtocolBufferException ex) {
                logger.log(Level.SEVERE, "Error decoding message type: " + msgType, ex);
            }
        }
    }  
   
    private void doAnalyze(MessageEvent e, AnalysisRequest request) {
        String text = request.getText();
        String analyzerFactory = DEFAULT_ANALYZER_FACTORY;
        if (request.hasAnalyzerFactory()) {
            analyzerFactory = request.getAnalyzerFactory();
        }
        Channel chan = e.getChannel();
        try {
            List<String> tokens = TextAnalyzer.analyze(text, analyzerFactory,
                                                       request.getAnalyzerArgsList().toArray(new String[0]));
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
                sendMessage(chan, MSG_ANALYSIS_RESULT, builder.build());
            }
            else {
                AnalysisResult.Builder builder = AnalysisResult.newBuilder();
                builder.setToken("");
                builder.setDone(1);
                sendMessage(chan, MSG_ANALYSIS_RESULT, builder.build());
            }
        }
        catch (Exception ex) {
            logger.log(Level.SEVERE, ex.getMessage(), ex);
            AnalysisError.Builder builder = AnalysisError.newBuilder();
            builder.setError (ex.getClass().getName());
            builder.setDescription(ex.getMessage());
            sendMessage(chan, MSG_ANALYSIS_ERROR, builder.build());
        }      
    }
   
    private void sendMessage(Channel chan, short msgType, MessageLite msg)
    {
        ChannelBuffer buffer = ChannelBuffers.dynamicBuffer();
        buffer.writeShort(msgType);
        buffer.writeBytes(msg.toByteArray());
        chan.write(buffer);
    }

}
