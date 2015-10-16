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
#ifndef _TASYNC_UTIL_H
#define _TASYNC_UTIL_H

#include <boost/smart_ptr.hpp>
#include <boost/function.hpp>
#include "concurrency/ThreadManager.h"

namespace apache { namespace thrift { namespace async {

class TAsyncUtil {
public:
	static boost::shared_ptr< apache::thrift::concurrency::ThreadManager > createThreadManager( int threadNum );
	static boost::shared_ptr< apache::thrift::concurrency::Runnable > createTask( const boost::function<void() >& functor );
	static int32_t readInt( const std::string& in, size_t& pos );
	static int32_t readInt( const std::string& in );
	static void writeInt( std::string& out, int32_t v ) ;
	static bool extractChannelMessage( std::string& msg_buf, int32_t& channelId, std::string& msg );
	
	
};

}}}

#endif /*_TASYNC_UTIL_H*/
 