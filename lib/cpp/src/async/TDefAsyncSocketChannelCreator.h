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

#ifndef _TDEF_ASYNC_SOCKET_CHANNEL_CREATOR_H
#define _TDEF_ASYNC_SOCKET_CHANNEL_CREATOR_H

#include <boost/asio.hpp>
#include "TAsyncSocketChannelCreator.h"
#include "TAsyncServerOrClientChannel.h"
#include "TAsyncSocketConnectionListener.h"

namespace apache { namespace thrift { namespace async {

class TDefAsyncSocketChannelCreator: public TAsyncSocketChannelCreator {
private:		
	struct SockChannelId {
		boost::shared_ptr< boost::asio::ip::tcp::socket > sock;
		int channelId;
		
		SockChannelId( const boost::shared_ptr< boost::asio::ip::tcp::socket >& _sock, int _channelId );
		
		
		bool operator==( const SockChannelId& right ) const;
		
		bool operator<( const SockChannelId& right ) const;
	};
	
public:
	typedef apache::thrift::async::TGenericAsyncChannel AsyncClientChannel;
	typedef apache::thrift::async::TAsyncServerChannel AsyncServerChannel;
	typedef boost::function< void (const boost::shared_ptr<boost::asio::ip::tcp::socket>&, const boost::shared_ptr<BoostAsyncWriter>&, int, const boost::shared_ptr<AsyncServerChannel>& ) > ServerMessageWriterSetter;
	typedef boost::function< void( const boost::shared_ptr<boost::asio::ip::tcp::socket>&, const boost::shared_ptr<BoostAsyncWriter>&, int, const boost::shared_ptr<AsyncClientChannel>& ) > ClientMessageWriterSetter;
	
	virtual boost::shared_ptr<TGenericAsyncChannel> createClientChannel( const boost::shared_ptr< boost::asio::ip::tcp::socket >& sock,
                                                    const boost::shared_ptr<BoostAsyncWriter>& asyncWriter,
													int channelId,
													const boost::shared_ptr< apache::thrift::protocol::TProtocolFactory >& protoFactory,
													int timeoutMillis );
	virtual boost::shared_ptr<TAsyncServerChannel> createServerChannel( boost::shared_ptr<boost::asio::ip::tcp::socket> sock,
                                                const boost::shared_ptr<BoostAsyncWriter>& asyncWriter, 
												int channelId );
	void setServerMessageWriterSetter( const ServerMessageWriterSetter& setter );
	
	void setClientMessageWriterSetter( const ClientMessageWriterSetter& setter );
	
	TAsyncServerOrClientChannel findChannel( boost::shared_ptr<boost::asio::ip::tcp::socket> sock, 
												int channelId ) const;
	
	void removeChannelsOn( boost::shared_ptr<boost::asio::ip::tcp::socket> sock, 
				std::list< boost::shared_ptr<TGenericAsyncChannel> >& clientChannels,
				std::list< boost::shared_ptr<TAsyncServerChannel> >& serverChannels );
	void connectionLost( const  boost::shared_ptr<boost::asio::ip::tcp::socket>& sock, boost::shared_ptr<TAsyncSocketConnectionListener> connListener );
private:
	std::map<SockChannelId, TAsyncServerOrClientChannel> channels;
	ServerMessageWriterSetter serverMessageWriterSet;
	ClientMessageWriterSetter clientMessageWriterSet; 
};

}}}

#endif/*_TDEF_ASYNC_SOCKET_CHANNEL_CREATOR_H*/
