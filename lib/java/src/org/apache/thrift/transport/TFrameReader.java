package org.apache.thrift.transport;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

/**
 * Created by stou on 2017/8/17.
 */
public class TFrameReader {
    private SocketChannel channel;
    private ByteBuffer frameSizeBuffer = ByteBuffer.allocate( 4 );
    private ByteBuffer frameBuffer = null;

    public TFrameReader( SocketChannel channel) {
        this.channel = channel;

    }
    public ByteBuffer readFrom() throws  IOException{
        if( frameSizeBuffer.remaining() != 0 ) {
            int n = channel.read( frameSizeBuffer );
            if( n == -1 ) throw new IOException( "End of stream");
            if( frameSizeBuffer.remaining() == 0 ) {
                n = TFramedTransport.decodeFrameSize(frameSizeBuffer.array());
                frameBuffer = ByteBuffer.allocate( n );
                if( n <= 0 ) {
                    System.out.println( "Error, frame size is " + n );
                } else {
                    n = channel.read(frameBuffer);
                    if (n == -1) throw new IOException("End of stream");
                }
            }
        } else {
            int n = channel.read( frameBuffer );
            if(  n == -1 ) throw new IOException( "End of stream");
        }

        if( frameSizeBuffer.remaining() == 0 && frameBuffer.remaining() == 0 ) {
            frameSizeBuffer.limit( frameSizeBuffer.capacity() );
            frameSizeBuffer.position( 0 );
            return frameBuffer;
        }
        return null;
    }
}
