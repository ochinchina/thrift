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
#include "TGenericAsyncChannel.h"
#include <concurrency/PlatformThreadFactory.h>

namespace apache { namespace thrift { namespace async {

class TGenericAsyncChannel::Task: public apache::thrift::concurrency::Runnable {
public:
	Task( const boost::function<void()>& func )
	:func_( func )
	{
	}
	void run() {
		func_();
	}
private:
	boost::function<void()> func_;
};


TGenericAsyncChannel::TGenericAsyncChannel( boost::shared_ptr< ::apache::thrift::protocol::TProtocolFactory > protocolFactory,
                                                int timeoutMillis)
:TAsyncDispatchableChannel( protocolFactory, timeoutMillis )
{
	timerManager_.threadFactory(boost::shared_ptr<apache::thrift::concurrency::PlatformThreadFactory>(new apache::thrift::concurrency::PlatformThreadFactory()));
  	timerManager_.start();
}
	 
void TGenericAsyncChannel::setMessageWriter( const MessageWriter & msgWirter ) {
	this->msgWriter_ = msgWirter;
}
	
void TGenericAsyncChannel::sendMessage( const std::string& msg, const boost::function< void( bool ) >& callback ) {
	msgWriter_( msg, callback );
}

void TGenericAsyncChannel::startTimer( int timeoutInMillis, const boost::function<void()>& callback ) {
	timerManager_.add( boost::shared_ptr<apache::thrift::concurrency::Runnable>(new Task( callback )), timeoutInMillis );
}

}}}
