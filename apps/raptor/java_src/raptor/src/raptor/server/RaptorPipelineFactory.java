package raptor.server;

import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.handler.codec.frame.LengthFieldBasedFrameDecoder;
import org.jboss.netty.handler.codec.frame.LengthFieldPrepender;
import org.jboss.netty.handler.codec.protobuf.ProtobufEncoder;

class RaptorPipelineFactory implements ChannelPipelineFactory {

    public ChannelPipeline getPipeline() throws Exception {
        ChannelPipeline p = Channels.pipeline();
        // Decoders (incoming data)
        p.addLast("frameDecoder", new LengthFieldBasedFrameDecoder(1048576, 0, 4, 0, 4));
        // Encoders (outgoing data)
        p.addLast("frameEncoder", new LengthFieldPrepender(4));
        p.addLast("protobufEncoder", new ProtobufEncoder());
        p.addLast("handler", new RaptorHandler());
        return p;
    }
}
