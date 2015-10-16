#include <boost/asio.hpp>
#include <protocol/TBinaryProtocol.h>
#include <async/TGenericAsyncChannel.h>
#include <async/TAsyncSocketConnectionListener.h>
#include <async/TAsyncSocketMuxServer.h>
#include "../gen-cpp/Calculator.h"

using namespace tutorial;
using namespace apache::thrift::async;
using namespace apache::thrift::protocol;

class MyConnectionListener: public TAsyncSocketConnectionListener, public boost::enable_shared_from_this<MyConnectionListener> {
public:
	virtual void connectionLost( const boost::shared_ptr<boost::asio::ip::tcp::socket>& sock ) {
	}
	virtual void connectionLost( const boost::shared_ptr<TGenericAsyncChannel>& clientChannel ) {
	}
	
	virtual void connectionLost( const boost::shared_ptr<TAsyncServerChannel>& serverChannel ) {
	}
	
	virtual void connectionEstablished( const boost::shared_ptr<boost::asio::ip::tcp::socket>& sock,
					TAsyncSocketChannelCreator& channelCreator ) {
		int channelId = 1;
		boost::shared_ptr< TProtocolFactory > protoFactory( new TBinaryProtocolFactory() );
		boost::shared_ptr<TGenericAsyncChannel> channel = channelCreator.createClientChannel( sock, channelId, protoFactory, 2000 );
		  
		boost::shared_ptr<CalculatorAsyncClient> client( new CalculatorAsyncClient(channel, protoFactory) );
		
		doSomething( client );		
	}
	
	void doSomething( boost::shared_ptr<CalculatorAsyncClient>& client ) {
		client->ping( boost::bind( &MyConnectionListener::pingResult, shared_from_this(), _1, client ) );  	
	}
	
	void pingResult( CalculatorCobClient& cobClient, boost::shared_ptr<CalculatorAsyncClient> asyncClient ) {
		cobClient.recv_ping();
		std::cout << "get the ping result" << std::endl;
		for( int i = 0; i < 10; i++ ) {
			for( int j = 0; j < 10; j++ ) {
				asyncClient->add( boost::bind( &MyConnectionListener::addResult, shared_from_this(), _1, asyncClient ), i, j);
			}
		}
	}
	
	void addResult( CalculatorCobClient& cobClient, boost::shared_ptr<CalculatorAsyncClient> asyncClient ) {
		int32_t r = cobClient.recv_add();
		std::cout << "get the add result:" << r << std::endl;
	}
};

int main( int argc, char** argv ) {
	boost::asio::io_service io_service;
	
	TAsyncSocketMuxServer server( io_service, "0.0.0.0", "9090", 1 );

	boost::shared_ptr<MyConnectionListener> listener( new MyConnectionListener() );	
	server.setConnectionListener( listener );
	server.start();
	
	io_service.run();
	
	return 0;
	
}

