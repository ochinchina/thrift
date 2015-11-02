import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;

import org.apache.thrift.transport.TNonblockingMemoryTransport.MessageWriter;
import org.apache.thrift.transport.TNonblockingTransport.AsyncWriteCallback;


public class SocketChannelMessageWriter implements MessageWriter {

	private Socket sock;
	private int channel;
	
	public SocketChannelMessageWriter( Socket sock, int channel ) {
		this.sock = sock;
		this.channel = channel;
	}
	
	@Override
	public void write(ByteBuffer msg, AsyncWriteCallback cb) {
		boolean success = false;
		
		System.out.println( "write msg " + msg );
		synchronized( sock ) {			
			try {
				writeInt( sock.getOutputStream(), channel );
				sock.getOutputStream().write(msg.array(), msg.arrayOffset(), msg.remaining());
				success = true;
			}catch( Exception ex ) {
			}
		}
		cb.writeFinished(success);
	}

	private void writeInt(OutputStream out, int channel) throws IOException {
		byte[] b = new byte[4];
		b[0] = (byte)(( channel >> 24 ) & 0xff);
		b[1] = (byte)(( channel >> 16 ) & 0xff);
		b[2] = (byte)(( channel >> 8 ) & 0xff);
		b[3] = (byte)(( channel ) & 0xff);
		out.write(b);
	}
}
