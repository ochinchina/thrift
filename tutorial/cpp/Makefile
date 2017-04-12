#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied. See the License for the
# specific language governing permissions and limitations
# under the License.
#

#BOOST_DIR = /usr/local/boost/include/boost-1_33_1/
#THRIFT_DIR = /usr/local/include/thrift

GEN_SRC = ../gen-cpp/SharedService.cpp ../gen-cpp/shared_types.cpp ../gen-cpp/tutorial_types.cpp ../gen-cpp/Calculator.cpp

ECHO_GEN_SRC=../gen-cpp/EchoService.cpp ../gen-cpp/echo_types.cpp

CPP_FLAGS=-DHAVE_NETINET_IN_H

LIBS = -L${THRIFT_LIB_DIR} -L${BOOST_LIB_DIR} -lthrift -lthriftnb -lboost_thread -lboost_system -levent

default: server client echo_server echo_client

server: CppServer.cpp
	g++ -o CppServer -I${THRIFT_DIR} -I${BOOST_DIR}  ${CPP_FLAGS} -I../gen-cpp CppServer.cpp ${GEN_SRC} ${LIBS}

client: CppClient.cpp
	g++ -o CppClient -I${THRIFT_DIR} -I${BOOST_DIR}  ${CPP_FLAGS} -I../gen-cpp CppClient.cpp ${GEN_SRC} ${LIBS}

echo_server: AsyncEchoServer.cpp
	g++ -o echo_server  -I${THRIFT_DIR} -I${BOOST_DIR}  ${CPP_FLAGS} -I../gen-cpp AsyncEchoServer.cpp ${ECHO_GEN_SRC} ${LIBS}
echo_client: AsyncEchoClient.cpp
	g++ -o echo_client  -I${THRIFT_DIR} -I${BOOST_DIR}  ${CPP_FLAGS} -I../gen-cpp AsyncEchoClient.cpp ${ECHO_GEN_SRC} ${LIBS}
clean:
	$(RM) -r CppClient CppServer echo_server echo_client 
