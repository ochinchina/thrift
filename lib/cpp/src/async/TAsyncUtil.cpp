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
#include "TAsyncUtil.h"
#include "concurrency/PlatformThreadFactory.h"

namespace apache { namespace thrift { namespace async {

namespace {
	class Task: public apache::thrift::concurrency::Runnable {
	public:
		Task( const boost::function<void()>& func )
		:func_( func )
		{
		}
	
		virtual void run() {
			func_();
		}
	private:
		boost::function<void()> func_;
	};
}

boost::shared_ptr< apache::thrift::concurrency::ThreadManager > TAsyncUtil::createThreadManager( int threadNum ) {
	boost::shared_ptr<apache::thrift::concurrency::ThreadManager> threadManager = apache::thrift::concurrency::ThreadManager::newThreadManager();
	threadManager->threadFactory( boost::shared_ptr<apache::thrift::concurrency::PlatformThreadFactory>( new apache::thrift::concurrency::PlatformThreadFactory() ) );
    threadManager->addWorker( threadNum );
    threadManager->start();
    return threadManager;
}

boost::shared_ptr< apache::thrift::concurrency::Runnable > TAsyncUtil::createTask( const boost::function<void() >& functor ) {
	return boost::shared_ptr< apache::thrift::concurrency::Runnable >( new Task( functor ) );
}

int32_t TAsyncUtil::readInt( const std::string& in, size_t& pos ) {
	return ( ( in[pos++] & 0xff ) << 24 ) | ( ( in[pos++] & 0xff ) << 16 ) | ( ( in[pos++] & 0xff ) << 8 ) | ( in[pos++] & 0xff );
}

int32_t TAsyncUtil::readInt( const std::string& in ) {
	size_t pos = 0;
	return readInt( in, pos );
}

void TAsyncUtil::writeInt( std::string& out, int32_t v ) {
	char buf[4];
	
	buf[0] = (char)( ( v >> 24 ) & 0xff );
	buf[1] = (char)( ( v >> 16 ) & 0xff );
	buf[2] = (char)( ( v >> 8 ) & 0xff );
	buf[3] = (char)( v & 0xff );
	
	out.append( buf, 4 );
}

}}}
