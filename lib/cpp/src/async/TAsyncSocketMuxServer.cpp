/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
#include "TAsyncSocketMuxServer.h"
#include "BackGroundIOService.h"
#include "TAsyncUtil.h"

namespace apache { namespace thrift { namespace async {

TAsyncSocketMuxServer::TAsyncSocketMuxServer( boost::asio::io_service& io_service,
				const std::string& listenAddr, 
				const std::string& listenPort,
				const boost::shared_ptr<apache::thrift::concurrency::ThreadManager>& threadManager )
:listenAddr_( listenAddr ),
listenPort_( listenPort ),
threadManager_( threadManager ),
io_service_(  io_service ),
acceptor_( io_service_ )
{
}

TAsyncSocketMuxServer::TAsyncSocketMuxServer( const std::string& listenAddr, 
				const std::string& listenPort,
				const boost::shared_ptr<apache::thrift::concurrency::ThreadManager>& threadManager )
:listenAddr_( listenAddr ),
listenPort_( listenPort ),
threadManager_( threadManager ),
io_service_(  BackGroundIOService::getInstance().get_io_service() ),
acceptor_( io_service_ )
{
}

TAsyncSocketMuxServer::TAsyncSocketMuxServer( const std::string& listenAddr, 
			const std::string& listenPort,
			int threadNum )
:listenAddr_( listenAddr ),
listenPort_( listenPort ),
threadManager_( TAsyncUtil::createThreadManager( threadNum ) ),
io_service_(  BackGroundIOService::getInstance().get_io_service() ),
acceptor_( io_service_ )
{
}
TAsyncSocketMuxServer::TAsyncSocketMuxServer( boost::asio::io_service& io_service, 
			const std::string& listenAddr, 
			const std::string& listenPort,
			int threadNum )
:listenAddr_( listenAddr ),
listenPort_( listenPort ),
threadManager_( TAsyncUtil::createThreadManager( threadNum ) ),
io_service_(  io_service ),
acceptor_( io_service_ )
{
}


void TAsyncSocketMuxServer::setConnectionListener( boost::shared_ptr<TAsyncSocketConnectionListener> listener ) {
	connListener_ = listener;
}
void TAsyncSocketMuxServer::start() {
 
 	channelCreator_.setClientMessageWriterSetter( boost::bind( &TAsyncSocketMuxServer::setClientMessageWriter, this, _1, _2, _3 ) );
 	channelCreator_.setServerMessageWriterSetter( boost::bind( &TAsyncSocketMuxServer::setServerMessageWriter, this, _1, _2, _3 ) );
 	
	boost::asio::ip::tcp::resolver::query query( listenAddr_, listenPort_ );
	boost::asio::ip::tcp::resolver resolver(io_service_);
	boost::system::error_code ec;

	boost::asio::ip::tcp::resolver::iterator iter = resolver.resolve( query, ec );
	
	if( ec ) {
		throw std::runtime_error( "fail to solve the listening address");
	}

	for( ; iter != boost::asio::ip::tcp::resolver::iterator(); iter++ ) {
		boost::asio::ip::tcp::endpoint ep = *iter;
		acceptor_.close( ec );
		acceptor_.open( ep.protocol(), ec );
		acceptor_.bind( ep, ec );
		if( !ec ) break;
	}
	
	acceptor_.listen( 10, ec );

	if( ec ) throw  std::runtime_error("fail to listen");
	startAccept();
}

void TAsyncSocketMuxServer::setClientMessageWriter( const boost::shared_ptr<boost::asio::ip::tcp::socket>& sock, int channelId, const boost::shared_ptr<TGenericAsyncChannel>& clientChannel ) {
	clientChannel->setMessageWriter( boost::bind( &TAsyncSocketMuxServer::write, this, sock, _1, _2, channelId ) );
}

void TAsyncSocketMuxServer::setServerMessageWriter( const boost::shared_ptr<boost::asio::ip::tcp::socket>& sock, int channelId, const boost::shared_ptr<TAsyncServerChannel>& channel ) {
	channel->setMessageWriter( boost::bind( &TAsyncSocketMuxServer::write, this, sock, _1, _2, channelId ) );
}

void TAsyncSocketMuxServer::startAccept() {
	boost::shared_ptr< boost::asio::ip::tcp::socket> sock ( new boost::asio::ip::tcp::socket( io_service_ ) );
	acceptor_.async_accept( *sock, boost::bind( &TAsyncSocketMuxServer::connectionAccepted, this, boost::asio::placeholders::error, sock ) );
}

void TAsyncSocketMuxServer::connectionAccepted( const boost::system::error_code& ec, boost::shared_ptr< boost::asio::ip::tcp::socket> sock ) {

	if( !ec ) {
		connListener_->connectionEstablished( sock, channelCreator_ );
		
		startRead( sock, new char[4096], 4096, new std::string() );
		
		startAccept();
	}

}

void TAsyncSocketMuxServer::startRead( boost::shared_ptr< boost::asio::ip::tcp::socket > sock,
					char* tmp_buf,
					size_t tmp_buf_len,
					std::string* msg_buf ) {
	boost::asio::async_read( *sock,
				boost::asio::buffer(tmp_buf, tmp_buf_len ),
				boost::asio::transfer_at_least(1),
				boost::bind( &TAsyncSocketMuxServer::dataRecevied, this, sock, _1, _2, tmp_buf, tmp_buf_len, msg_buf ) );
}

void TAsyncSocketMuxServer::dataRecevied( boost::shared_ptr< boost::asio::ip::tcp::socket > sock,											
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
		
		int32_t channelId;
		std::string msg;
		
		while( TAsyncUtil::extractChannelMessage( *msg_buf, channelId, msg ) ) {
			if( threadManager_ ) {
				threadManager_->add( TAsyncUtil::createTask( boost::bind( &TAsyncSocketMuxServer::processMessage, this, sock, channelId, msg ) ) );
			} else {
				processMessage( sock, channelId, msg );
			}
		}
				
		startRead( sock, tmp_buf, tmp_buf_len, msg_buf );
	}	
}

void TAsyncSocketMuxServer::processMessage( boost::shared_ptr< boost::asio::ip::tcp::socket > sock,
				int32_t channelId,
				std::string msg ) {
	TAsyncServerOrClientChannel channel = channelCreator_.findChannel( sock, channelId );			
	channel.recvMessage( msg );
}



void TAsyncSocketMuxServer::write( boost::shared_ptr< boost::asio::ip::tcp::socket> sock, 
		const std::string& msg, 
		const boost::function< void( bool ) >& callback,
		int channelId ) {
		
	boost::shared_ptr<std::string> s( new std::string() );
	TAsyncUtil::writeInt( *s, channelId );
	TAsyncUtil::writeInt( *s, msg.length() );
	s->append( msg );
	boost::asio::async_write( *sock,
                            boost::asio::buffer( s->data(), s->length() ),
                            boost::asio::transfer_all(),
                            boost::bind( &TAsyncSocketMuxServer::sendFinished, _1, _2, s, callback ) );
}

void TAsyncSocketMuxServer::sendFinished( const boost::system::error_code& error,
                                std::size_t bytes_transferred,
                                boost::shared_ptr< std::string > s,
								boost::function< void( bool ) > callback ) {
	if( !callback.empty() ) {
		callback( !error );
	}
}


}}}