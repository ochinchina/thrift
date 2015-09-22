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

#include "../gen-cpp/Calculator.h"

using namespace std;
using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;

using namespace tutorial;
using namespace shared;

using namespace boost;

class AysncCalculatorHandler : public CalculatorCobSvIf {
 public:
  AysncCalculatorHandler() {}

  void ping( std::tr1::function<void()> cob ) {
    printf("ping()\n");
    cob();
  }

  void add(std::tr1::function<void(int32_t const& _return)> cob, const int32_t n1, const int32_t n2) {
    printf("add(%d,%d)\n", n1, n2);
    cob( n1 + n2 );
  }

  void calculate(std::tr1::function<void(int32_t const& _return)> cob, std::tr1::function<void(::apache::thrift::TDelayedException* _throw)> exn_cob, const int32_t logid, const Work& work) {
    printf("calculate(%d,{%d,%d,%d})\n", logid, work.op, work.num1, work.num2);
    int32_t val;

    switch (work.op) {
    case Operation::ADD:
      val = work.num1 + work.num2;
      break;
    case Operation::SUBTRACT:
      val = work.num1 - work.num2;
      break;
    case Operation::MULTIPLY:
      val = work.num1 * work.num2;
      break;
    case Operation::DIVIDE:
      if (work.num2 == 0) {
        InvalidOperation io;
        io.what = work.op;
        io.why = "Cannot divide by 0";
        exn_cob( new apache::thrift::TExceptionWrapper<InvalidOperation>( io ) );
      } else {
      	val = work.num1 / work.num2;
      }
      
      break;
    default:
      InvalidOperation io;
      io.what = work.op;
      io.why = "Invalid Operation";
      exn_cob( new apache::thrift::TExceptionWrapper<InvalidOperation>( io ) );
    }

    SharedStruct ss;
    ss.key = logid;
    char buffer[12];
    snprintf(buffer, sizeof(buffer), "%d", val);
    ss.value = buffer;

    log[logid] = ss;

    cob( val );
  }

  void getStruct(std::tr1::function<void(SharedStruct const& _return)> cob, const int32_t logid ) {
    printf("getStruct(%d)\n", logid);
    cob( log[logid] );
  }

  void zip(std::tr1::function<void()> cob) {
    printf("zip()\n");
    cob();
  }

protected:
  map<int32_t, SharedStruct> log;

};

int main(int argc, char **argv) {

  async::TSocketAsyncServer server("localhost",
  						"9090",
						shared_ptr<TProtocolFactory>( new TBinaryProtocolFactory() ),
						shared_ptr<CalculatorAsyncProcessor>(new CalculatorAsyncProcessor( shared_ptr<AysncCalculatorHandler>(new AysncCalculatorHandler()) ) ),
						1 );

  printf("Starting the server...\n");
  server.serve();
  printf("done.\n");
  return 0;
}

