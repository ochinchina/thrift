
#include <async/TAsyncServerChannel.h>
#include <async/TGenericAsyncChannel.h>
#include <async/TGenericAsyncServer.h>
#include <async/TAsyncSocketMuxClient.h>
#include <concurrency/ThreadManager.h>
#include <concurrency/BoostThreadFactory.h>
#include <protocol/TBinaryProtocol.h>
#include <boost/asio.hpp>
#include "../gen-cpp/Calculator.h"
#include <map>
#include <boost/bind.hpp>

using std::map;
using namespace apache::thrift::async;
using namespace apache::thrift::concurrency;
using namespace apache::thrift::protocol;
using namespace tutorial;
using namespace shared;
using boost::shared_ptr;

class AysncCalculatorHandler : public CalculatorCobSvIf {
 public:
  AysncCalculatorHandler() {}

  void ping( std::tr1::function<void()> cob ) {
    printf("ping()\n");
    cob();
  }

  void add(std::tr1::function<void(int32_t const& _return)> cob, const int32_t n1, const int32_t n2) {
    printf("add(%d,%d)\n", n1, n2);
    cob( n1 + n2 );
  }

  void calculate(std::tr1::function<void(int32_t const& _return)> cob, std::tr1::function<void(::apache::thrift::TDelayedException* _throw)> exn_cob, const int32_t logid, const Work& work) {
    printf("calculate(%d,{%d,%d,%d})\n", logid, work.op, work.num1, work.num2);
    int32_t val;

    switch (work.op) {
    case Operation::ADD:
      val = work.num1 + work.num2;
      break;
    case Operation::SUBTRACT:
      val = work.num1 - work.num2;
      break;
    case Operation::MULTIPLY:
      val = work.num1 * work.num2;
      break;
    case Operation::DIVIDE:
      if (work.num2 == 0) {
        InvalidOperation io;
        io.what = work.op;
        io.why = "Cannot divide by 0";
        exn_cob( new apache::thrift::TExceptionWrapper<InvalidOperation>( io ) );
      } else {
      	val = work.num1 / work.num2;
      }
      
      break;
    default:
      InvalidOperation io;
      io.what = work.op;
      io.why = "Invalid Operation";
      exn_cob( new apache::thrift::TExceptionWrapper<InvalidOperation>( io ) );
    }

    SharedStruct ss;
    ss.key = logid;
    char buffer[12];
    snprintf(buffer, sizeof(buffer), "%d", val);
    ss.value = buffer;

    log[logid] = ss;

    cob( val );
  }

  void getStruct(std::tr1::function<void(SharedStruct const& _return)> cob, const int32_t logid ) {
    printf("getStruct(%d)\n", logid);
    cob( log[logid] );
  }

  void zip(std::tr1::function<void()> cob) {
    printf("zip()\n");
    cob();
  }

protected:
  map<int32_t, SharedStruct> log;

};


class AsyncGenericClientTest: public TAsyncSocketConnectionListener, public boost::enable_shared_from_this<AsyncGenericClientTest> {
public:
	typedef apache::thrift::async::TGenericAsyncChannel AsyncClientChannel;
	typedef apache::thrift::async::TAsyncServerChannel AsyncServerChannel;
	
	AsyncGenericClientTest( boost::asio::io_service& io_service,
			const std::string& serverAddr,
			const std::string& serverPort )
	:muxClient_( io_service, serverAddr, serverPort, 0 ),
	server_( shared_ptr<TProtocolFactory>( new TBinaryProtocolFactory() ),
							shared_ptr<CalculatorAsyncProcessor>(new CalculatorAsyncProcessor( shared_ptr<AysncCalculatorHandler>(new AysncCalculatorHandler()) ) ),
							1 )
	{
	}
		
	void start() {
		muxClient_.setSocketConnectionListener( shared_from_this() );		
		muxClient_.start();
	}	
	
	virtual void connectionLost( const boost::shared_ptr<boost::asio::ip::tcp::socket>& sock ) {
	}
	virtual void connectionLost( const boost::shared_ptr<TGenericAsyncChannel>& clientChannel ) {
	}
	
	virtual void connectionLost( const boost::shared_ptr<TAsyncServerChannel>& serverChannel ) {
	}
	
	virtual void connectionEstablished( const boost::shared_ptr<boost::asio::ip::tcp::socket>& sock,
					TAsyncSocketChannelCreator& channelCreator ) {
		boost::shared_ptr<TAsyncServerChannel> channel = channelCreator.createServerChannel( sock, 1 );
		server_.connectionAccepted( channel );
	}

private:
	TAsyncSocketMuxClient muxClient_;
	TGenericAsyncServer server_;
};


int main( int argc, char** argv ) {
	boost::asio::io_service io_service;
	
	boost::shared_ptr<AsyncGenericClientTest> test( new AsyncGenericClientTest(io_service, argv[1], argv[2] ));
	
	test->start();
	
	io_service.run();
}
