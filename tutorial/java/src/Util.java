import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.AbstractMap;
import java.util.Map;


public class Util {
	public static Map.Entry<ByteBuffer, Integer> readChannelMessage( InputStream in ) throws IOException {
		int channel = readInt( in );		
		int len = readInt( in );
		ByteBuffer msg = readBytes( in, len );
		return new AbstractMap.SimpleImmutableEntry<ByteBuffer, Integer>(msg, channel);
	}

	public static ByteBuffer readBytes(InputStream in, int len) throws IOException {
		byte[] b = new byte[len];
		int offset = 0;
		
		while( len > 0 ) {
			int n = in.read( b, offset, len );
			if( n <= 0 ) throw new IOException( "End of stream");
			offset += n;
			len -= n;
		}
		
		return ByteBuffer.wrap( b  );
	}

	public static int readInt(InputStream in) throws IOException {
		int n = 0;
		
		for( int i = 0; i < 4; i++ ) {
			n <<= 8;
			int t = in.read();
			if( t == -1 ) throw new IOException( "End of stream");
			n |= t & 0xff;
		}
		
		return n;
	}
}
