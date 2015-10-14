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
#include "TAsyncServerOrClientChannel.h"

namespace apache { namespace thrift { namespace async {

TAsyncServerOrClientChannel::TAsyncServerOrClientChannel() {
}
		
TAsyncServerOrClientChannel::TAsyncServerOrClientChannel( const boost::shared_ptr<AsyncClientChannel>& _clientChannel )
:clientChannel( _clientChannel )
{
}

TAsyncServerOrClientChannel::TAsyncServerOrClientChannel( const boost::shared_ptr<AsyncServerChannel>& _serverChannel )
:serverChannel( _serverChannel )
{
}

boost::shared_ptr<TAsyncServerOrClientChannel::AsyncClientChannel> TAsyncServerOrClientChannel::getClientChannel() const {
	return clientChannel;
}

boost::shared_ptr<TAsyncServerOrClientChannel::AsyncServerChannel> TAsyncServerOrClientChannel::getServerChannel() const {
	return serverChannel;
}
 
bool TAsyncServerOrClientChannel::isClientChannel() const {
	return clientChannel;
}
bool TAsyncServerOrClientChannel::isServerChannel() const {
	return serverChannel;
}

void TAsyncServerOrClientChannel::recvMessage( const std::string& msg ) {
	if( clientChannel ) {
		clientChannel->recvMessage( msg );
	} else if( serverChannel ) {
		serverChannel->recvMessage( msg );
	}
}

}}}
