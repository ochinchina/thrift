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
#include "TDefAsyncSocketChannelCreator.h"

namespace apache { namespace thrift { namespace async {

		
TDefAsyncSocketChannelCreator::SockChannelId::SockChannelId( const boost::shared_ptr< boost::asio::ip::tcp::socket >& _sock, int _channelId )
:sock( _sock ),
channelId( _channelId )
{
}

bool TDefAsyncSocketChannelCreator::SockChannelId::operator==( const SockChannelId& right ) const {
	return channelId == right.channelId && sock == right.sock;
}

bool TDefAsyncSocketChannelCreator::SockChannelId::operator<( const SockChannelId& right ) const {
	if( channelId == right.channelId ) {
		return this->sock < right.sock;
	} else {
		return channelId < right.channelId;
	}
}

boost::shared_ptr<TGenericAsyncChannel> TDefAsyncSocketChannelCreator::createClientChannel( const boost::shared_ptr< boost::asio::ip::tcp::socket >& sock,
												int channelId,
												const boost::shared_ptr< apache::thrift::protocol::TProtocolFactory >& protoFactory,
												int timeoutMillis ) {
	boost::shared_ptr<TGenericAsyncChannel> channel( new TGenericAsyncChannel( protoFactory, timeoutMillis ) );
	clientMessageWriterSet( sock, channelId, channel );
	channels[ SockChannelId( sock, channelId ) ] = 	TAsyncServerOrClientChannel( channel );
	return channel;				
}


boost::shared_ptr<TAsyncServerChannel> TDefAsyncSocketChannelCreator::createServerChannel( boost::shared_ptr<boost::asio::ip::tcp::socket> sock, 
											int channelId ) {
	boost::shared_ptr<TAsyncServerChannel> channel( new TAsyncServerChannel() );
	serverMessageWriterSet( sock, channelId, channel );
	channels[ SockChannelId( sock, channelId ) ] = 	TAsyncServerOrClientChannel( channel );
	return channel;
}


void TDefAsyncSocketChannelCreator::setServerMessageWriterSetter( const ServerMessageWriterSetter& setter ) {
	serverMessageWriterSet = setter;  
}

void TDefAsyncSocketChannelCreator::setClientMessageWriterSetter( const ClientMessageWriterSetter& setter ) {
	clientMessageWriterSet = setter;
}

TAsyncServerOrClientChannel TDefAsyncSocketChannelCreator::findChannel( boost::shared_ptr<boost::asio::ip::tcp::socket> sock, 
											int channelId ) const {
	SockChannelId sockChannelId( sock, channelId );
	
	std::map<SockChannelId, TAsyncServerOrClientChannel>::const_iterator iter = channels.find( sockChannelId  );
	if( iter == channels.end() ) {
		return TAsyncServerOrClientChannel(); 
	} else {
		return iter->second;
	}
} 

void TDefAsyncSocketChannelCreator::removeChannelsOn( boost::shared_ptr<boost::asio::ip::tcp::socket> sock, 
			std::list< boost::shared_ptr<TGenericAsyncChannel> >& clientChannels,
			std::list< boost::shared_ptr<TAsyncServerChannel> >& serverChannels ) {
	std::map<SockChannelId, TAsyncServerOrClientChannel>::const_iterator iter;		
	std::list< SockChannelId> removedChannels;
	
	for( iter = channels.begin(); iter !=  channels.end(); iter++ ) {
		if( iter->first.sock == sock ) {
			removedChannels.push_back( iter->first );
		}
	}
	
	for( std::list<SockChannelId>::const_iterator iter2 = removedChannels.begin(); iter2 != removedChannels.end(); iter2++ ) {
		TAsyncServerOrClientChannel channel = channels[ *iter2 ];
		if( channel.isClientChannel() ) {
			clientChannels.push_back( channel.getClientChannel() );
		} else if( channel.isServerChannel()) {
			serverChannels.push_back( channel.getServerChannel() );
		}
		channels.erase( *iter2 );
	}
}

}}}
