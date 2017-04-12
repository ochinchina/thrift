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

#include "TGenericAsyncServer.h"
#include "TAsyncUtil.h"
#include <concurrency/BoostThreadFactory.h>
#include <concurrency/Thread.h>
#include <boost/bind.hpp>

namespace apache { namespace thrift { namespace async {



TGenericAsyncServer::TGenericAsyncServer( const boost::shared_ptr<apache::thrift::protocol::TProtocolFactory>& protoFactory, 
				const boost::shared_ptr<apache::thrift::async::TAsyncProcessor>& processor,
				boost::shared_ptr<apache::thrift::concurrency::ThreadManager> threadMgr ) 
:protoFactory_( protoFactory ),
processor_( processor ),
threadManager_( threadMgr )
{
}

TGenericAsyncServer::TGenericAsyncServer( const boost::shared_ptr<apache::thrift::protocol::TProtocolFactory>& protoFactory, 
				const boost::shared_ptr<apache::thrift::async::TAsyncProcessor>& processor,
				int processorThreads ) 
:protoFactory_( protoFactory ),
processor_( processor ),
threadManager_( TAsyncUtil::createThreadManager( processorThreads )  )
{
}	

void TGenericAsyncServer::async_serve() {
}

void TGenericAsyncServer::shutdown() {
}

void TGenericAsyncServer::connectionAccepted( boost::shared_ptr<TAsyncServerChannel> channel ) {
	channel->setMessageListener( boost::bind( &TGenericAsyncServer::messageReceived, this, channel, _1 ) );
}		
	
void TGenericAsyncServer::messageReceived( boost::shared_ptr<TAsyncServerChannel> channel, const std::string& msg  ) {
	boost::shared_ptr<apache::thrift::transport::TMemoryBuffer> reqBuf( new apache::thrift::transport::TMemoryBuffer() );
	reqBuf->write( (const uint8_t*)msg.data(), msg.size() );
	if( threadManager_ ) {
		threadManager_->add( TAsyncUtil::createTask( boost::bind( &TGenericAsyncServer::processRequest, this, channel, reqBuf ) ) );
	} else {
		processRequest( channel, reqBuf );
	}
}	

void TGenericAsyncServer::processRequest(  boost::shared_ptr< TAsyncServerChannel > channel,
			boost::shared_ptr<apache::thrift::transport::TMemoryBuffer> reqBuf ) {
	boost::shared_ptr<apache::thrift::transport::TMemoryBuffer> outBuf(new apache::thrift::transport::TMemoryBuffer());
	boost::shared_ptr<apache::thrift::protocol::TProtocol> iprot(protoFactory_->getProtocol(reqBuf));
	boost::shared_ptr<apache::thrift::protocol::TProtocol> oprot(protoFactory_->getProtocol(outBuf));
	processor_->process( boost::bind( &TGenericAsyncServer::processCompleted, this, channel,  _1, outBuf ),
                                        iprot,
                                        oprot);
                                    

}

void TGenericAsyncServer::processCompleted( boost::shared_ptr< TAsyncServerChannel > channel,			bool success,
			boost::shared_ptr<apache::thrift::transport::TMemoryBuffer> outBuf ) {
	uint8_t* bufPtr =0;
	uint32_t sz = 0;

	outBuf->getBuffer( &bufPtr, &sz );
	channel->write( std::string( (const char*)bufPtr, sz ), boost::bind( &TGenericAsyncServer::writeFinished, _1 ) );

}

void TGenericAsyncServer::writeFinished( bool success ) {
}
	
}}}
