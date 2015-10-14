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
#ifndef _TASYNC_SERVER_OR_CLIENT_CHANNEL_H
#define _TASYNC_SERVER_OR_CLIENT_CHANNEL_H

#include "TGenericAsyncChannel.h"
#include "TAsyncServerChannel.h"

namespace apache { namespace thrift { namespace async {
 
class TAsyncServerOrClientChannel {
public:
	typedef apache::thrift::async::TGenericAsyncChannel AsyncClientChannel;
	typedef apache::thrift::async::TAsyncServerChannel AsyncServerChannel;

	TAsyncServerOrClientChannel();
			
	TAsyncServerOrClientChannel( const boost::shared_ptr<AsyncClientChannel>& _clientChannel );
	
	TAsyncServerOrClientChannel( const boost::shared_ptr<AsyncServerChannel>& _serverChannel );
	
	boost::shared_ptr<AsyncClientChannel> getClientChannel() const;
	
	boost::shared_ptr<AsyncServerChannel> getServerChannel() const;
	 
	bool isClientChannel() const;
	bool isServerChannel() const;
	
	void recvMessage( const std::string& msg );

private:
	boost::shared_ptr<AsyncClientChannel> clientChannel;
	boost::shared_ptr<AsyncServerChannel> serverChannel;
};

}}}

#endif/*_TASYNC_SERVER_OR_CLIENT_CHANNEL_H*/

