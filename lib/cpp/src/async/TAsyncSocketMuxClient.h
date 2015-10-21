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
#ifndef _TASYNC_SOCKET_MUX_CLIENT_H
#define _TASYNC_SOCKET_MUX_CLIENT_H

#include <boost/asio.hpp>
#include <concurrency/ThreadManager.h>
#include "TAsyncSocketConnectionListener.h"
#include "TGenericAsyncChannel.h"
#include "TAsyncServerChannel.h"
#include "TDefAsyncSocketChannelCreator.h"

namespace apache { namespace thrift { namespace async {

class TAsyncSocketMuxClient {
public:
	typedef apache::thrift::async::TGenericAsyncChannel AsyncClientChannel;
	typedef apache::thrift::async::TAsyncServerChannel AsyncServerChannel;
	
	TAsyncSocketMuxClient( boost::asio::io_service& io_service,
			const std::string& serverAddr,
			const std::string& serverPort,
			boost::shared_ptr<apache::thrift::concurrency::ThreadManager> threadManager );
	
	TAsyncSocketMuxClient( boost::asio::io_service& io_service,
			const std::string& serverAddr,
			const std::string& serverPort,
			int threadNum );
	
	void setSocketConnectionListener( const boost::shared_ptr<TAsyncSocketConnectionListener>& listener );
	void start();
private:
	void setClientMessageWriter( const boost::shared_ptr<boost::asio::ip::tcp::socket>& sock, 
							int channelId,
							const boost::shared_ptr<AsyncClientChannel>& channel );
		
	void setServerMessageWriter( const boost::shared_ptr<boost::asio::ip::tcp::socket>& sock, 
							int channelId,
							const boost::shared_ptr<AsyncServerChannel>& channel );

	void startConnect() ;
	
	void handleConnect( const boost::system::error_code& error ) ;
		
	
	void startRead( char* buf, size_t size ) ;
	
	void dataReceived( const boost::system::error_code& error,
                        std::size_t bytes_transferred,
                        char* buf,
                        size_t buf_size );
	
	void processPackets();
	
	void write( const std::string& msg,
				const boost::function< void( bool ) >& callback, 
				int32_t channelId );
	
	
	void processPacket( int32_t channelId, std::string msg );
	
	static void sendFinished( const boost::system::error_code& error,
                                std::size_t bytes_transferred,
                                boost::shared_ptr< std::string > s,
								boost::function< void( bool ) > callback );
	
private:
	std::string serverAddr_;
	std::string serverPort_;
	std::string recvData_;
	boost::asio::io_service& io_service_;
	boost::shared_ptr<boost::asio::ip::tcp::socket> sock_;
	boost::shared_ptr<apache::thrift::concurrency::ThreadManager> threadManager_;
	boost::shared_ptr<TAsyncSocketConnectionListener> listener_;
	TDefAsyncSocketChannelCreator channelCreator_;
};

}}}

#endif/*_TASYNC_SOCKET_MUX_CLIENT_H*/
