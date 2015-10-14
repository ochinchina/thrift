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
#include "TAsyncSocketMuxClient.h"
#include "TAsyncUtil.h"
#include <concurrency/PlatformThreadFactory.h>

namespace apache { namespace thrift { namespace async {

TAsyncSocketMuxClient::TAsyncSocketMuxClient( boost::asio::io_service& io_service,
		const std::string& serverAddr,
		const std::string& serverPort,
		boost::shared_ptr<apache::thrift::concurrency::ThreadManager> threadManager )
:io_service_( io_service ),
sock_( new boost::asio::ip::tcp::socket( io_service ) ),
serverAddr_( serverAddr ),
serverPort_( serverPort ),
threadManager_( threadManager )
{
}

TAsyncSocketMuxClient::TAsyncSocketMuxClient( boost::asio::io_service& io_service,
		const std::string& serverAddr,
		const std::string& serverPort,
		int threadNum )
:io_service_( io_service ),
sock_( new boost::asio::ip::tcp::socket( io_service ) ),
serverAddr_( serverAddr ),
serverPort_( serverPort ),
threadManager_( TAsyncUtil::createThreadManager( threadNum ) )
{
}

void TAsyncSocketMuxClient::setSocketConnectionListener( const boost::shared_ptr<TAsyncSocketConnectionListener>& listener ) {
	listener_ = listener;
}

void TAsyncSocketMuxClient::start() {
	channelCreator_.setClientMessageWriterSetter( boost::bind( &TAsyncSocketMuxClient::setClientMessageWriter, this, _1, _2, _3 ) );
	channelCreator_.setServerMessageWriterSetter( boost::bind( &TAsyncSocketMuxClient::setServerMessageWriter, this, _1, _2, _3 ) );
	startConnect();
}

void TAsyncSocketMuxClient::setClientMessageWriter( const boost::shared_ptr<boost::asio::ip::tcp::socket>& sock, 
						int channelId,
						const boost::shared_ptr<AsyncClientChannel>& channel ) {
	channel->setMessageWriter( boost::bind( &TAsyncSocketMuxClient::write, this, _1, _2, channelId ) );

}
	
void TAsyncSocketMuxClient::setServerMessageWriter( const boost::shared_ptr<boost::asio::ip::tcp::socket>& sock, 
						int channelId,
						const boost::shared_ptr<AsyncServerChannel>& channel ) {
	channel->setMessageWriter( boost::bind( &TAsyncSocketMuxClient::write, this, _1, _2, channelId ) );	
}

void TAsyncSocketMuxClient::startConnect() {
	boost::asio::ip::tcp::resolver resolver( io_service_ );
    boost::asio::ip::tcp::resolver::query query( serverAddr_, serverPort_ );
    boost::system::error_code err;
    boost::asio::ip::tcp::resolver::iterator iter = resolver.resolve( query, err );
    
    if( err || iter == boost::asio::ip::tcp::resolver::iterator() ) {
    	
    } else {
        sock_->close( err );
        boost::asio::ip::tcp::endpoint endpoint = *iter;
        sock_->async_connect( endpoint, boost::bind( &TAsyncSocketMuxClient::handleConnect, this, _1 ) );
    }
}

void TAsyncSocketMuxClient::handleConnect( const boost::system::error_code& error ) {
	if( error ) {
		startConnect();
	} else {
		listener_->connectionEstablished( sock_, channelCreator_ );
		startRead( new char[4096], 4096 );
	}
}
	

void TAsyncSocketMuxClient::startRead( char* buf, size_t size ) {
	sock_->async_read_some( boost::asio::buffer( buf, size),
                            boost::bind( &TAsyncSocketMuxClient::dataReceived, this, _1, _2, buf, size ) );
}

void TAsyncSocketMuxClient::dataReceived( const boost::system::error_code& error,
                    std::size_t bytes_transferred,
                    char* buf,
                    size_t buf_size ) {
    if( error ) {
    	delete[] buf;
		recvData_.clear();
		removeChannels();                
		startConnect();
    }else{
        recvData_.append(  buf, bytes_transferred );
        startRead( buf, buf_size );
        processPackets();
    }
}

void TAsyncSocketMuxClient::removeChannels( ) {
	std::list< boost::shared_ptr<TGenericAsyncChannel> > clientChannels;
	std::list< boost::shared_ptr<TAsyncServerChannel> > serverChannels;
	channelCreator_.removeChannelsOn( sock_, clientChannels, serverChannels );
	
	listener_->connectionLost( sock_ );
	for( std::list< boost::shared_ptr<TGenericAsyncChannel> >::const_iterator iter = clientChannels.begin(); iter != clientChannels.end(); iter++ ) {
		listener_->connectionLost( *iter );
	}
	
	for( std::list< boost::shared_ptr<TAsyncServerChannel> >::const_iterator iter = serverChannels.begin(); iter != serverChannels.end(); iter++ ) {
		listener_->connectionLost( *iter );
	}  	
}

void TAsyncSocketMuxClient::processPackets() {
	size_t pos = 0;
	
	while( recvData_.length() > 8 ) {
            //read channel
            int32_t channelId = TAsyncUtil::readInt( recvData_, pos );
            int32_t n = TAsyncUtil::readInt( recvData_, pos );
            if( recvData_.length() < ( 8 + n ) ) {
                    break;
            } else {
                    std::string msg = recvData_.substr( 8, n );
                    recvData_.erase( 0, 8 + n );
					threadManager_->add( TAsyncUtil::createTask( boost::bind( &TAsyncSocketMuxClient::processPacket, this, channelId, msg ) ) );
            }
    }
}

void TAsyncSocketMuxClient::write( const std::string& msg,
			const boost::function< void( bool ) >& callback, 
			int32_t channelId ) {
	boost::shared_ptr<std::string> s( new std::string() );
	
	TAsyncUtil::writeInt( *s, channelId );
	s->append( msg );
	boost::asio::async_write( *sock_,
                        boost::asio::buffer( s->data(), s->length() ),
                        boost::asio::transfer_all(),
                        boost::bind( &sendFinished, _1, _2, s, callback ) );
	
}

void TAsyncSocketMuxClient::processPacket( int32_t channelId, std::string msg ) {
	TAsyncServerOrClientChannel channel = channelCreator_.findChannel( sock_, channelId );
	
	channel.recvMessage( msg );		
}


void TAsyncSocketMuxClient::sendFinished( const boost::system::error_code& error,
                            std::size_t bytes_transferred,
                            boost::shared_ptr< std::string > s,
							boost::function< void( bool ) > callback ) {
	if( !callback.empty() ) {
		callback( !error );
	}
}


}}}
