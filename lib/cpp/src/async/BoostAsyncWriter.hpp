#ifndef _BOOST_ASYNC_WRITER_HPP
#define _BOOST_ASYNC_WRITER_HPP

#include <queue>
#include <iostream>
#include <string>
#include <boost/asio.hpp>
#include <boost/function.hpp>
#include <boost/thread.hpp>
#include <utility>
	
namespace apache { namespace thrift { namespace async {

class BoostAsyncWriter: public boost::enable_shared_from_this<BoostAsyncWriter> {

public:
    BoostAsyncWriter( const boost::shared_ptr<boost::asio::ip::tcp::socket>& _socket )
	:shutdown_( false ),
	socket_( _socket ),
	strand_( _socket->get_io_service() )	
    {
    }

    void write( const std::string& message, const boost::function< void( bool ) >& resultCb ){
        strand_.post( boost::bind( &BoostAsyncWriter::writeImpl, shared_from_this(), message, resultCb ) );
    }
    
    void shutdown( const boost::function<void()>& shutdownCallback ) {
    	strand_.post( boost::bind( &BoostAsyncWriter::handleShutdown, shared_from_this(), shutdownCallback ) );
	}
	
	void shutdown() {
		shutdown( boost::bind( &BoostAsyncWriter::ignoreShutdownResult ) );
	}
    
private:
	void handleShutdown( boost::function<void()> shutdownCallback ) {
		shutdown_ = true;		
		shutdownCallback();
	}
	
    void writeImpl( std::string message, boost::function< void( bool ) > resultCb ) {
    	//return immediatelly if already shutdown
    	if( shutdown_ ) {
    		resultCb( false );
		} else {		
	        outQueue_.push( std::make_pair(message, resultCb) );
	        if ( outQueue_.size() == 1 ) {
	        	this->write();
	        }
        }
    }

    void write() {
        const std::string& message = outQueue_.front().first;
        boost::asio::async_write( *socket_,
                boost::asio::buffer( message.data(), message.size() ),
                strand_.wrap( boost::bind( &BoostAsyncWriter::handleWrite, shared_from_this(), boost::asio::placeholders::error, boost::asio::placeholders::bytes_transferred ) )
                );
    }

    void handleWrite( const boost::system::error_code& error, const size_t bytesTransferred ) {
    	std::pair<std::string, WriteResultCallback >& firstElem = outQueue_.front();
    	if( error || bytesTransferred >= firstElem.first.size() ) {
    		boost::function< void( bool ) > resultCb = firstElem.second;
    		outQueue_.pop();
    		if( shutdown_ ) {
	        	resultCb( !error );                
	        	clearQueue();
				return ;
			}
	
			if ( !outQueue_.empty() ) {
	            this->write();            
	        }
	
			resultCb( !error );
		} else {
			//write remain
			firstElem.first.erase(0, bytesTransferred );
			this->write();
		} 
    }
    
    void clearQueue() {
    	while( !outQueue_.empty() ) {
			outQueue_.front().second( false );
			outQueue_.pop();
		}
	}
	
	static void ignoreShutdownResult() {
	}

private:
	typedef boost::function< void( bool ) > WriteResultCallback;
	volatile bool shutdown_;
    boost::shared_ptr<boost::asio::ip::tcp::socket> socket_;
    boost::asio::strand strand_;
    std::queue< std::pair<std::string, WriteResultCallback > > outQueue_;
};

}}}//end namesapce

#endif/*_BOOST_ASYNC_WRITER_HPP*/

