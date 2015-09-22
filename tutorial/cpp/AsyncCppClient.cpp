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

#include <stdio.h>
#include <unistd.h>
#include <sys/time.h>

#include <protocol/TBinaryProtocol.h>
#include <transport/TSocket.h>
#include <transport/TTransportUtils.h>
#include <boost/smart_ptr.hpp>
#include <async/TAsyncSocketChannel.h>

#include "../gen-cpp/Calculator.h"

using namespace std;
using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;

using namespace tutorial;
using namespace shared;

using namespace boost;


void addResult( CalculatorCobClient& cobClient, int n1, int n2 ) {
	std::cout << n1 << "+" << n2 << "=" << cobClient.recv_add() << std::endl;
	
}

void connectCallback( CalculatorAsyncClient& client ) {
    for( int i = 0; i < 10; i++ ) {
       for( int j = 0; j < 10; j++ ) {
         client.add( std::tr1::bind( &addResult, std::tr1::placeholders::_1, i, j ), i, j );
       }
    }

}


int main(int argc, char** argv) {
  boost::shared_ptr< apache::thrift::protocol::TProtocolFactory > protoFactory( new apache::thrift::protocol::TBinaryProtocolFactory() );
  boost::shared_ptr<async::TAsyncSocketChannel> channel( new async::TAsyncSocketChannel( "localhost", "9090", protoFactory, 0, 1 ) );
  CalculatorAsyncClient client(channel, protoFactory );

  try {
    channel->start( boost::bind( &connectCallback, client ) );

  } catch (TException &tx) {
    printf("ERROR: %s\n", tx.what());
  }

  ::sleep( 5 );

}
