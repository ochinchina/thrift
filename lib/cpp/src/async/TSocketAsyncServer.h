#ifndef _TSOCKET_ASYNC_SERVER_HPP
#define _TSOCKET_ASYNC_SERVER_HPP
 
#include <async/TAsyncProcessor.h>
#include <async/BoostAsyncWriter.hpp>
#include <protocol/TProtocol.h>
#include <transport/TBufferTransports.h>
#include <concurrency/ThreadPool.h>
#include <boost/asio.hpp>

namespace apache { namespace thrift { namespace async {
class TSocketAsyncServer {
public:
	
	TSocketAsyncServer( const std::string& addr, 
			const std::string& port,
			const boost::shared_ptr<apache::thrift::protocol::TProtocolFactory>& protoFactory, 
			const boost::shared_ptr<apache::thrift::async::TAsyncProcessor>& processor,
			int processorThreads );
	void serve();
private:
	void startAccept();

	void connectionAccepted( const boost::system::error_code& ec, boost::shared_ptr< boost::asio::ip::tcp::socket> sock );
	
	void startRead( boost::shared_ptr< boost::asio::ip::tcp::socket > sock,
                        boost::shared_ptr<BoostAsyncWriter> asyncWriter,
						char* tmp_buf,
						size_t tmp_buf_len,
						std::string* msg_buf );
						
	void dataRecevied( boost::shared_ptr< boost::asio::ip::tcp::socket > sock,
                        boost::shared_ptr<BoostAsyncWriter> asyncWriter,											
						const boost::system::error_code& error,
						size_t bytes_read,
						char* tmp_buf,
						size_t tmp_buf_len,
						std::string* msg_buf );
						
	boost::shared_ptr<apache::thrift::transport::TMemoryBuffer> extractFrame( std::string& msg_buf );
	
	void processCompleted( boost::shared_ptr< boost::asio::ip::tcp::socket> sock,
                boost::shared_ptr<BoostAsyncWriter> asyncWriter,
				bool success,
				boost::shared_ptr<apache::thrift::transport::TMemoryBuffer> outBuf );
	void processRequest( boost::shared_ptr< boost::asio::ip::tcp::socket > sock,
                boost::shared_ptr<BoostAsyncWriter> asyncWriter,
				boost::shared_ptr<apache::thrift::transport::TMemoryBuffer> reqBuf );
				
	//static void writeFinished( const boost::system::error_code& error,  boost::shared_ptr<apache::thrift::transport::TMemoryBuffer> outBuf );	
    static void writeFinished( bool success );
private:
	std::string listenAddr_;
	std::string listenPort_;
	boost::asio::io_service io_service_;
	boost::asio::ip::tcp::acceptor acceptor_;
	boost::shared_ptr<apache::thrift::protocol::TProtocolFactory> protoFactory_;
	boost::shared_ptr<apache::thrift::async::TAsyncProcessor> processor_;
	boost::shared_ptr<apache::thrift::concurrency::ThreadPool> threadPool_;
	class Task;
};

}}}//end namespace
#endif/*_TSOCKET_ASYNC_SERVER_HPP*/
