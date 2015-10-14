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
#ifndef _TASYNC_SERVER_CHANNEL_H
#define _TASYNC_SERVER_CHANNEL_H

#include <boost/function.hpp>
#include <string>
  
namespace apache { namespace thrift { namespace async {


class TAsyncServerChannel {
public:
	typedef boost::function< void( bool ) > MessageSendCallback;
	typedef boost::function< void (const std::string&, const MessageSendCallback& ) > MessageWriter;

	typedef boost::function< void (const std::string& ) > MessageListener;	
	
	void setMessageListener( const MessageListener& msgListener );
	 
	void setMessageWriter( const MessageWriter& msgWriter );
	
	void recvMessage( const std::string& msg ) ;
	
	void write( const std::string& msg, const MessageSendCallback& callback );
private:
	MessageListener msgListener_;
	MessageWriter msgWriter_;
};
}}}

#endif/*_TASYNC_SERVER_CHANNEL_H*/

