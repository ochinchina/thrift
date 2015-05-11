
import java.util.concurrent.Executors;

import org.apache.thrift.server.TNonblockingServer;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TTransportException;

import tutorial.Calculator;


public class JavaAsyncServer {
	public static void main( String...args ) throws TTransportException {
		TNonblockingServerSocket socket = new TNonblockingServerSocket(9090);
		Calculator.AsyncProcessor processor = new Calculator.AsyncProcessor( new AsyncCalculatorHandler() );
		TNonblockingServer server = new TNonblockingServer( new TNonblockingServer.Args(socket).processor(processor), Executors.newFixedThreadPool(2) );
		server.serve();
	}
}

