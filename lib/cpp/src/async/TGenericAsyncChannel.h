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

#ifndef _TGENERIC_ASYNC_CHANNEL_H
#define _TGENERIC_ASYNC_CHANNEL_H

#include "TAsyncDispatchableChannel.h"
#include <concurrency/TimerManager.h>


using apache::thrift::concurrency::TimerManager;

namespace apache { namespace thrift { namespace async {

class TGenericAsyncChannel: public TAsyncDispatchableChannel {
public:
	typedef boost::function< void( bool ) > MessageSendCallback;
	typedef boost::function< void (const std::string&, const MessageSendCallback& ) > MessageWriter;

	TGenericAsyncChannel( boost::shared_ptr< ::apache::thrift::protocol::TProtocolFactory > protocolFactory,
                                                int timeoutMillis );
		 
	void setMessageWriter( const MessageWriter & msgWirter );

	/**
	 *  @Override TAsyncDispatchableChannel::sendMessage
	 */	 			
	virtual void sendMessage( const std::string& msg, const boost::function< void( bool ) >& callback );
	
	/**
	 *  @Override TAsyncDispatchableChannel::startTimer
	 */	 			
	virtual void startTimer( int timeoutInMillis, const boost::function<void()>& callback );
	
	virtual void recvMessage( const std::string& msg ) {
		TAsyncDispatchableChannel::recvMessage( msg );
	}
private:
	MessageWriter msgWriter_;
	TimerManager timerManager_;
	class Task;	
};

}}}

#endif/*_TGENERIC_ASYNC_CHANNEL_H*/

