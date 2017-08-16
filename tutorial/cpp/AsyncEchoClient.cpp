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

#include "../gen-cpp/EchoService.h"

using namespace std;
using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;

using namespace echo;

using namespace boost;


class Counter {
public:
    Counter( int initial_value = 0)
    :count( initial_value )
    {
    }
    void inc() {
        boost::lock_guard<boost::mutex> guard( mutex_ );
        count ++;
    }
    
    void dec() {
        boost::lock_guard<boost::mutex> guard( mutex_ );
        count --;
    }
    
    int get() {
        boost::lock_guard<boost::mutex> guard( mutex_ );
        return count;
    }
    

private:
    boost::mutex mutex_;
    int count;
};


Counter unfinished;
Counter errors;
volatile bool connected = false;

void echoResult( EchoServiceCobClient& echoClient ) {
    /*std::string msg;
    echoClient.recv_echo( msg );
    std::cout << msg << std::endl;*/
    try {
        unfinished.dec();
        std::string msg;
    
        echoClient.recv_echo( msg );
        //std::cout << msg << std::endl;
    }catch( ... ) {
        errors.inc();
    }
	
}

std::string create_big_message() {
    std::string msg("---\nthis is a message\n");
    
    for( int i = 0; i < 1000; i++ ) {
        msg.append( "this is a message\n");
    }
    msg.append( "---\n");
    return msg;
}
void connectCallback( EchoServiceAsyncClient& client, int loops ) {
    connected = true;
}


int main(int argc, char** argv) {
  boost::shared_ptr< apache::thrift::protocol::TProtocolFactory > protoFactory( new apache::thrift::protocol::TBinaryProtocolFactory() );
  int threads = 5;
  boost::shared_ptr<async::TAsyncSocketChannel> channel( new async::TAsyncSocketChannel( "localhost", "9090", protoFactory, 0, threads ) );
  EchoServiceAsyncClient client(channel, protoFactory );
  

  try {
    
    int loops = atoi( argv[1] ); 

    channel->start( boost::bind( &connectCallback, client, loops  ) );
    while( !connected );
    
    time_t start = time( 0 );
    std::string msg = create_big_message();
     
    for( int i = 0; i < loops; i++ ) {
        while( unfinished.get() > 1000 ) {
            ::usleep( 10 );
        }
        unfinished.inc();
        client.echo( boost::bind( &echoResult, _1), msg, 2000 );
    }
    
    while( unfinished.get() > 0 ) {
        ::sleep( 1 );
    }
    
    time_t end = time( 0 );
    
    int total = end - start;
    std::cout << "total time:" << total << ",errors:" << errors.get() << std::endl; 
 
  } catch (TException &tx) {
    printf("ERROR: %s\n", tx.what());
  }
  
  for( ; ; ) {
    ::sleep( 5 );
  }

}
