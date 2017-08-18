package org.apache.thrift.transport;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * Created by stou on 2017/8/11.
 */
public class TNettyMessageDecoder extends ByteToMessageDecoder {
    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        //decode the message
        while( in.readableBytes() >= 4 ) {
            int n = in.getInt(in.readerIndex());

            if( in.readableBytes() >= 4 + n ) {
                byte[] b = new byte[n];
                in.skipBytes( 4 );
                in.readBytes( b );
                out.add( ByteBuffer.wrap( b ));
            } else {
                break;
            }
        }
    }
}
