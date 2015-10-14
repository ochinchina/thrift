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
#ifndef _TASYNC_SOCKET_CHANNEL_CREATOR_H
#define _TASYNC_SOCKET_CHANNEL_CREATOR_H

#include "TGenericAsyncChannel.h"
#include "TAsyncServerChannel.h"
#include <boost/smart_ptr.hpp>
#include <boost/asio.hpp>

namespace apache { namespace thrift { namespace async { 
class TAsyncSocketChannelCreator {
public:
	typedef TGenericAsyncChannel AsyncClientChannel;
	typedef TAsyncServerChannel AsyncServerChannel;
	
	virtual ~TAsyncSocketChannelCreator(){};
	virtual boost::shared_ptr<AsyncClientChannel> createClientChannel( const boost::shared_ptr< boost::asio::ip::tcp::socket >& sock,
													int channelId,
													const boost::shared_ptr< apache::thrift::protocol::TProtocolFactory >& protoFactory,
													int timeoutMillis ) = 0;
		
	virtual boost::shared_ptr<AsyncServerChannel> createServerChannel( boost::shared_ptr<boost::asio::ip::tcp::socket> sock, 
												int channelId ) = 0;
	
};

}}}

#endif/*_TASYNC_SOCKET_CHANNEL_CREATOR_H*/
