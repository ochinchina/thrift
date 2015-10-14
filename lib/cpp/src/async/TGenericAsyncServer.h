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
#ifndef _TGENERIC_ASYNC_SERVER_H
#define _TGENERIC_ASYNC_SERVER_H

#include "TAsyncServerChannel.h"
#include <boost/smart_ptr.hpp>
#include <async/TAsyncProcessor.h>
#include <protocol/TProtocol.h>
#include <transport/TBufferTransports.h>
#include <concurrency/ThreadManager.h>

namespace apache { namespace thrift { namespace async {

class TGenericAsyncServer {
public:
	TGenericAsyncServer( const boost::shared_ptr<apache::thrift::protocol::TProtocolFactory>& protoFactory, 
			const boost::shared_ptr<apache::thrift::async::TAsyncProcessor>& processor,
			boost::shared_ptr<apache::thrift::concurrency::ThreadManager> threadMgr );
	
	TGenericAsyncServer( const boost::shared_ptr<apache::thrift::protocol::TProtocolFactory>& protoFactory, 
			const boost::shared_ptr<apache::thrift::async::TAsyncProcessor>& processor,
			int processorThreads );
	
	void connectionAccepted( boost::shared_ptr<TAsyncServerChannel> channel );
	
	void async_serve();
	
	void shutdown();
private:	
	void messageReceived( boost::shared_ptr<TAsyncServerChannel> channel, const std::string& msg  );	
	
	void processRequest(  boost::shared_ptr< TAsyncServerChannel > channel,
				boost::shared_ptr<apache::thrift::transport::TMemoryBuffer> reqBuf );
	
	void processCompleted( boost::shared_ptr< TAsyncServerChannel > channel,
				bool success,
				boost::shared_ptr<apache::thrift::transport::TMemoryBuffer> outBuf );
	static void writeFinished( bool success );
private:
	boost::shared_ptr<apache::thrift::protocol::TProtocolFactory> protoFactory_;
	boost::shared_ptr<apache::thrift::async::TAsyncProcessor> processor_;
	boost::shared_ptr<apache::thrift::concurrency::ThreadManager> threadManager_;
	class Task;
};

}}}

#endif/*_TGENERIC_ASYNC_SERVER_H*/

