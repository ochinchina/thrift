import java.io.IOException;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TNonblockingSocket;
import org.apache.thrift.transport.TSelector;

import tutorial.Calculator.AsyncClient.add_call;
import tutorial.Calculator.AsyncClient.ping_call;


public class JavaAsyncClient {
	public static void main( String...args ) throws IOException, TException, InterruptedException {
		
		tutorial.Calculator.AsyncClient.Factory factory = new tutorial.Calculator.AsyncClient.Factory(  new TBinaryProtocol.Factory() );
		TSelector selector = new TSelector();
		TNonblockingSocket socket = new TNonblockingSocket("127.0.0.1", 9090, selector);
		tutorial.Calculator.AsyncClient client = factory.getAsyncClient( socket );
		client.setTimeout(2000);
		
		client.ping( new org.apache.thrift.async.AsyncMethodCallback< tutorial.Calculator.AsyncClient.ping_call>(){

			@Override
			public void onComplete(ping_call response) {
				System.out.println( "ping completed");
				try {
					response.getResult();
				} catch (TException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}

			@Override
			public void onError(Exception exception) {
				exception.printStackTrace();
				
			}
			
		});
		
		for( int i = 0; i < 10; i ++ ) {
			for( int j = 0; j < 10; j++ ) {
				final int a = i, b = j;
				client.add( i, j, new org.apache.thrift.async.AsyncMethodCallback< tutorial.Calculator.AsyncClient.add_call>(){
		
					@Override
					public void onComplete(add_call response) {
						try {
							System.out.println( a + "," + b + "=" + response.getResult() );
						} catch (TException e) {
							e.printStackTrace();
						}
						
					}
		
					@Override
					public void onError(Exception e) {
						e.printStackTrace();
					}
					
				});
				//Thread.sleep( 100 );
			}
		}
		
		System.out.println( "start to wait for the result");
		Thread.sleep( 6000 );
	}

	
}

