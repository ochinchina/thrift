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
#ifndef _TASYNC_SOCKET_MUX_SERVER_H
#define _TASYNC_SOCKET_MUX_SERVER_H

#include <boost/asio.hpp>
#include <concurrency/ThreadManager.h>
#include "TAsyncSocketConnectionListener.h"
#include "TDefAsyncSocketChannelCreator.h" 
 
namespace apache { namespace thrift { namespace async {

class TAsyncSocketMuxServer {
public:
	TAsyncSocketMuxServer( const std::string& listenAddr, 
			const std::string& listenPort,
			const boost::shared_ptr<apache::thrift::concurrency::ThreadManager>& threadManager );
	TAsyncSocketMuxServer( boost::asio::io_service& io_service, 
			const std::string& listenAddr, 
			const std::string& listenPort,
			const boost::shared_ptr<apache::thrift::concurrency::ThreadManager>& threadManager );
	TAsyncSocketMuxServer( const std::string& listenAddr, 
			const std::string& listenPort,
			int threadNum );
	TAsyncSocketMuxServer( boost::asio::io_service& io_service, 
			const std::string& listenAddr, 
			const std::string& listenPort,
			int threadNum );
	void setConnectionListener( boost::shared_ptr<TAsyncSocketConnectionListener> listener );
	void start();
private:
	void setClientMessageWriter( const boost::shared_ptr<boost::asio::ip::tcp::socket>& sock, const boost::shared_ptr<BoostAsyncWriter>& asyncWriter, int channelId, const boost::shared_ptr<TGenericAsyncChannel>& clientChannel );
	void setServerMessageWriter( const boost::shared_ptr<boost::asio::ip::tcp::socket>& sock, const boost::shared_ptr<BoostAsyncWriter>& asyncWriter, int channelId, const boost::shared_ptr<TAsyncServerChannel>& channel );
	void startAccept() ;
	
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
	
	void processMessage( boost::shared_ptr< boost::asio::ip::tcp::socket > sock,
                boost::shared_ptr<BoostAsyncWriter> asyncWriter,
				int32_t channelId,
				std::string msg );
	
	void write( boost::shared_ptr< boost::asio::ip::tcp::socket> sock,
            boost::shared_ptr<BoostAsyncWriter> asyncWriter, 
			const std::string& msg, 
			const boost::function< void( bool ) >& callback,
			int channelId );
	
	static void sendFinished( bool success,
									boost::function< void( bool ) > callback ) ;
private:
	std::string listenAddr_;
	std::string listenPort_;
	boost::shared_ptr<apache::thrift::concurrency::ThreadManager> threadManager_;
	boost::asio::io_service& io_service_;
	boost::asio::ip::tcp::acceptor acceptor_;
	boost::shared_ptr<TAsyncSocketConnectionListener> connListener_;
	TDefAsyncSocketChannelCreator channelCreator_;	
};

}}}

#endif/*_TASYNC_SOCKET_MUX_SERVER_H*/

