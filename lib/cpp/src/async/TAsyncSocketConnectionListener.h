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

#ifndef _TASYNC_SOCKET_CONNECTION_LISTENER_H
#define _TASYNC_SOCKET_CONNECTION_LISTENER_H

#include "TAsyncSocketChannelCreator.h"

namespace apache { namespace thrift { namespace async {
 
class TAsyncSocketConnectionListener {
public:
	virtual ~TAsyncSocketConnectionListener(){};
	virtual void connectionLost( const boost::shared_ptr<boost::asio::ip::tcp::socket>& sock ) = 0;
	virtual void connectionLost( const boost::shared_ptr<TGenericAsyncChannel>& clientChannel ) = 0;
	virtual void connectionLost( const boost::shared_ptr<TAsyncServerChannel>& serverChannel ) = 0;
	virtual void connectionEstablished( const boost::shared_ptr<boost::asio::ip::tcp::socket>& sock,
					TAsyncSocketChannelCreator& channelCreator ) = 0;
};

}}}

#endif/*_TASYNC_SOCKET_CONNECTION_LISTENER_H*/
