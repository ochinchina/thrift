import java.io.IOException;
import java.io.InputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.AbstractMap;
import java.util.Map;
import java.util.concurrent.Executors;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TNonblockingMemoryTransport;

import tutorial.Calculator.AsyncClient.add_call;


public class JavaAsyncMemoryServer {
	
	
	

	
	public static void main( String...args) throws IOException {
		
		
		try {
			ServerSocket serverSock = new ServerSocket( 9090 );
			for( ; ; ) {
				final Socket sock = serverSock.accept();
				new Thread() {
					public void run() {
						Map.Entry<TNonblockingMemoryTransport, tutorial.Calculator.AsyncClient > entry = startClient( sock, 1 );
						startRead( sock, entry.getKey() );
						try {
							doSomeOperation( entry.getValue() );
						} catch (TException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						} catch (InterruptedException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
				}.start();
				
			}
		}catch( Exception ex ) {
			ex.printStackTrace();
		}
	}	
	
	private static Map.Entry<TNonblockingMemoryTransport, tutorial.Calculator.AsyncClient > startClient(Socket sock, int channel )  {		
		TNonblockingMemoryTransport trans = transMgr.getTransport( sock, channel );
		
		if( trans == null ) {
			trans = transMgr.createTransport( sock, channel );
			trans.setMessageWriter( new SocketChannelMessageWriter( sock, channel ));
		}
		
		tutorial.Calculator.AsyncClient.Factory factory = new tutorial.Calculator.AsyncClient.Factory(  new TBinaryProtocol.Factory() );
		tutorial.Calculator.AsyncClient client = factory.getAsyncClient( trans );
		
		
		client.setTimeout(2000);
		client.setResponseProcThreadPool(Executors.newCachedThreadPool());
		return new AbstractMap.SimpleImmutableEntry<TNonblockingMemoryTransport, tutorial.Calculator.AsyncClient>( trans, client ) ;
	}

	private static void startRead( final Socket sock, final TNonblockingMemoryTransport trans) {
		new Thread() {
			public void run() {
				try {
					System.out.println( "start read from socket " + sock );
					InputStream in = sock.getInputStream();
					for( ; ; ) {						
						Map.Entry<ByteBuffer, Integer> msgWithChannel = Util.readChannelMessage(in);
						trans.messageReceived(msgWithChannel.getKey());
					}
				}catch( Exception e ) {
					e.printStackTrace();
					transMgr.removeTransports(sock);
				}
			}
		}.start();
		
	}
	
	private static void doSomeOperation( tutorial.Calculator.AsyncClient client ) throws TException, InterruptedException {
		
		/*client.ping( new org.apache.thrift.async.AsyncMethodCallback< tutorial.Calculator.AsyncClient.ping_call>(){
			@Override
			public void onComplete(ping_call response) {
				System.out.println( "ping completed");
				try {
					response.getResult();
				} catch (TException e) {					
					e.printStackTrace();
				}
			}

			@Override
			public void onError(Exception exception) {
				//exception.printStackTrace();
				
			}
			
		});*/
		
		
		for( int i = 0; i < 10; i ++ ) {
			for( int j = 0; j < 2; j++ ) {
				final int a = i, b = j;
				client.add( i, j, new org.apache.thrift.async.AsyncMethodCallback< tutorial.Calculator.AsyncClient.add_call>(){
		
					@Override
					public void onComplete(add_call response) {
						try {
							System.out.println( a + "," + b + "=" + response.getResult() );
						} catch (TException e) {
							//e.printStackTrace();
						}
						
					}
		
					@Override
					public void onError(Exception e) {
						e.printStackTrace();
					}
					
				});
				
			}
			try { Thread.sleep( 100 ); } catch( Exception ex ) {}
		}
		
		System.out.println( "start to wait for the result");
		Thread.sleep( 600000 );
		
	}

	
	
	private static SockChannelMemoryTransportManager transMgr = new SockChannelMemoryTransportManager();
	

	

}
