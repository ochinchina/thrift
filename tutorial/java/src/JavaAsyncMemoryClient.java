import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.AbstractMap;
import java.util.Map;
import java.util.concurrent.Executors;

import org.apache.thrift.TException;

import org.apache.thrift.server.TNonblockingServer;
import org.apache.thrift.transport.TNonblockingMemoryTransport;
import org.apache.thrift.transport.TNonblockingServerMemoryTransport;

import tutorial.Calculator;


public class JavaAsyncMemoryClient {
	public static void main( String...args ) throws IOException, TException, InterruptedException {
		final TNonblockingServerMemoryTransport transport = new TNonblockingServerMemoryTransport();
		new Thread() {
			public void run() {
				Calculator.AsyncProcessor processor = new Calculator.AsyncProcessor( new AsyncCalculatorHandler() );
				TNonblockingServer server = new TNonblockingServer( new TNonblockingServer.Args(transport).processor(processor), Executors.newFixedThreadPool(2) );
				server.serve();
			}
		}.start();
		
		Thread.sleep( 2000 );
		Socket sock = new Socket( "10.140.3.230", 9090 );
		int channel = 1;
		
		TNonblockingMemoryTransport trans = transMgr.createTransport( sock, channel );
		trans.setMessageWriter( new SocketChannelMessageWriter( sock, channel ) );
		transport.connectionAccepted(trans);
		
		startRead(sock, trans );
		
	}
	

	private static void startRead( final Socket sock, final TNonblockingMemoryTransport trans) {
		try {
			System.out.println( "start read from socket " + sock );
			InputStream in = sock.getInputStream();
			for( ; ; ) {						
				Map.Entry<ByteBuffer, Integer> msgWithChannel = Util.readChannelMessage(in);
				trans.messageReceived(msgWithChannel.getKey());
			}
		}catch( Exception e ) {
			e.printStackTrace();
			transMgr.removeTransport(sock, 1);
		}		
	}
	
	private static SockChannelMemoryTransportManager transMgr = new SockChannelMemoryTransportManager();

	
}
