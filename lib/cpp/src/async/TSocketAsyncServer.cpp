#include "TSocketAsyncServer.h"
#include <boost/function.hpp>
#include <boost/bind.hpp>
#include <concurrency/BoostThreadFactory.h>

namespace apache { namespace thrift { namespace async {

class TSocketAsyncServer::Task: public apache::thrift::concurrency::Runnable {
public:
	Task( const boost::function< void() >& func )
	:func_( func )
	{
	}
	virtual void run() {
		func_();
	}
private:
	boost::function< void() > func_;
};

TSocketAsyncServer::TSocketAsyncServer( const std::string& addr, 
		const std::string& port,
		const boost::shared_ptr<apache::thrift::protocol::TProtocolFactory>& protoFactory, 
		const boost::shared_ptr<apache::thrift::async::TAsyncProcessor>& processor,
		int processorThreads )
:listenAddr_( addr ),
listenPort_( port ),
acceptor_( io_service_ ),
protoFactory_( protoFactory ),
processor_( processor ),
threadManager_( apache::thrift::concurrency::ThreadManager::newThreadManager() )
{
	threadManager_->threadFactory( boost::shared_ptr<apache::thrift::concurrency::BoostThreadFactory>( new apache::thrift::concurrency::BoostThreadFactory() ) );
	threadManager_->start();
	threadManager_->addWorker( processorThreads );
}

void TSocketAsyncServer::serve() {
	boost::asio::ip::tcp::resolver::query query( listenAddr_, listenPort_ );
	boost::asio::ip::tcp::resolver resolver(io_service_);
	boost::system::error_code ec;

	boost::asio::ip::tcp::resolver::iterator iter = resolver.resolve( query, ec );
	
	if( ec ) {
		throw std::runtime_error( "fail to solve the listening address");
	}

	for( ; iter != boost::asio::ip::tcp::resolver::iterator(); iter++ ) {
		boost::asio::ip::tcp::endpoint ep = *iter;
		acceptor_.open( ep.protocol(), ec );
		acceptor_.bind( ep, ec );
		if( ec ) break;
	}
	
	acceptor_.listen( 128, ec );

	if( ec ) throw  std::runtime_error("fail to listen");
	startAccept();
	io_service_.run();
		
}

void TSocketAsyncServer::startAccept() {
	boost::shared_ptr< boost::asio::ip::tcp::socket> sock ( new boost::asio::ip::tcp::socket( io_service_ ) );
	acceptor_.async_accept( *sock, boost::bind( &TSocketAsyncServer::connectionAccepted, this, boost::asio::placeholders::error, sock ) );
}

void TSocketAsyncServer::connectionAccepted( const boost::system::error_code& ec, boost::shared_ptr< boost::asio::ip::tcp::socket> sock ) {

	if( !ec ) {
		startRead( sock, new char[4096], 4096, new std::string() );
		startAccept();
	}

}

void TSocketAsyncServer::startRead( boost::shared_ptr< boost::asio::ip::tcp::socket > sock,
						char* tmp_buf,
						size_t tmp_buf_len,
						std::string* msg_buf ) {
	boost::asio::async_read( *sock,
				boost::asio::buffer(tmp_buf, tmp_buf_len ),
				boost::asio::transfer_at_least(1),
				boost::bind( &TSocketAsyncServer::dataRecevied, this, sock, _1, _2, tmp_buf, tmp_buf_len, msg_buf ) );
}

void TSocketAsyncServer::dataRecevied( boost::shared_ptr< boost::asio::ip::tcp::socket > sock,											
						const boost::system::error_code& error,
						size_t bytes_read,
						char* tmp_buf,
						size_t tmp_buf_len,
						std::string* msg_buf ) {
	if( error ) {
		delete []tmp_buf;
		delete msg_buf;
	} else {
		msg_buf->append( tmp_buf, bytes_read );
		boost::shared_ptr<apache::thrift::transport::TMemoryBuffer> frame = extractFrame( *msg_buf );
		for( ;frame; frame = extractFrame( *msg_buf ) ) {
			threadManager_->add( boost::shared_ptr< Task >( new Task( boost::bind( &TSocketAsyncServer::processRequest, this, sock, frame ) ) ) );
		}
		startRead( sock, tmp_buf, tmp_buf_len, msg_buf );
	}	
}

void TSocketAsyncServer::processRequest(  boost::shared_ptr< boost::asio::ip::tcp::socket > sock,
				boost::shared_ptr<apache::thrift::transport::TMemoryBuffer> reqBuf ) {
	boost::shared_ptr<apache::thrift::transport::TMemoryBuffer>outBuf(new apache::thrift::transport::TMemoryBuffer());
	uint8_t lenBuf[4];
	outBuf->write( lenBuf, 4 );	
	boost::shared_ptr<apache::thrift::protocol::TProtocol> iprot(protoFactory_->getProtocol(reqBuf));
	boost::shared_ptr<apache::thrift::protocol::TProtocol> oprot(protoFactory_->getProtocol(outBuf));
	processor_->process( std::tr1::bind( &TSocketAsyncServer::processCompleted, this, sock,  std::tr1::placeholders::_1, outBuf ),
                                        iprot,
                                        oprot);

}

boost::shared_ptr<apache::thrift::transport::TMemoryBuffer> TSocketAsyncServer::extractFrame( std::string& msg_buf ) {
	if( msg_buf.length() < 4 ) {
		return boost::shared_ptr<apache::thrift::transport::TMemoryBuffer>();
	}
	
	size_t n = ( ( msg_buf[0] & 0xff ) << 24 ) | ( ( msg_buf[1] & 0xff ) << 16 ) | ( ( msg_buf[2] & 0xff ) << 8 ) | ( ( msg_buf[3] & 0xff ) );
	
	if( msg_buf.length() < ( 4 + n ) ) {
		return boost::shared_ptr<apache::thrift::transport::TMemoryBuffer>();
	}
	
	uint8_t* buf = new uint8_t[n];
	memcpy( buf, msg_buf.data() + 4, n );
	msg_buf.erase( 0, 4 + n );
	return boost::shared_ptr<apache::thrift::transport::TMemoryBuffer>( new apache::thrift::transport::TMemoryBuffer( buf, n, apache::thrift::transport::TMemoryBuffer::TAKE_OWNERSHIP ) );
}

void TSocketAsyncServer::processCompleted( boost::shared_ptr< boost::asio::ip::tcp::socket> sock,
				bool success,
				boost::shared_ptr<apache::thrift::transport::TMemoryBuffer> outBuf ) {
	uint8_t* bufPtr =0;
	uint32_t sz = 0;

	outBuf->getBuffer( &bufPtr, &sz );
	size_t n = sz - 4;

	bufPtr[0] = (uint8_t)( ( n >> 24 ) & 0xff ); 		
	bufPtr[1] = (uint8_t)( ( n >> 16 ) & 0xff );
	bufPtr[2] = (uint8_t)( ( n >> 8 ) & 0xff );
	bufPtr[3] = (uint8_t)( ( n ) & 0xff );
	
	boost::asio::async_write( *sock, 
					boost::asio::buffer( bufPtr, sz ),
					boost::bind( &writeFinished, boost::asio::placeholders::error, outBuf ) );

}

void TSocketAsyncServer::writeFinished( const boost::system::error_code& error,  boost::shared_ptr<apache::thrift::transport::TMemoryBuffer> outBuf ) {
}

}}}//end namesapce

