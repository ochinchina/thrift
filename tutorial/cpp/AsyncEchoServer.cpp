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

#include <protocol/TBinaryProtocol.h>
#include <async/TSocketAsyncServer.h>
#include <Thrift.h>

#include <iostream>
#include <stdexcept>
#include <sstream>

#include "../gen-cpp/EchoService.h"

using namespace std;
using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;

using namespace echo;

using namespace boost;

class AysncEchoHandler : public EchoServiceCobSvIf {
public:
  AysncEchoHandler() {}

  void echo(boost::function<void(std::string const& _return)> cob, const std::string& msg, int timeoutMillis = 0 ) {
    cob( msg );
  }

};

int main(int argc, char **argv) {

  int threads = 5;
  async::TSocketAsyncServer server("localhost",
  						"9090",
						shared_ptr<TProtocolFactory>( new TBinaryProtocolFactory() ),
						shared_ptr<EchoServiceAsyncProcessor>(new EchoServiceAsyncProcessor( shared_ptr<AysncEchoHandler>(new AysncEchoHandler()) ) ),
						threads );

  printf("Starting the server...\n");
  server.serve();
  printf("done.\n");
  return 0;
}

