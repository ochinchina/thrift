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

package org.apache.thrift.server;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TNonblockingServerTransport;
import org.apache.thrift.transport.TNonblockingTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides common methods and classes used by nonblocking TServer
 * implementations.
 */
public abstract class AbstractNonblockingServer extends TServer {
  protected final Logger LOGGER = LoggerFactory.getLogger(getClass().getName());

  public static abstract class AbstractNonblockingServerArgs<T extends AbstractNonblockingServerArgs<T>> extends AbstractServerArgs<T> {
    public long maxReadBufferBytes = Long.MAX_VALUE;

    public AbstractNonblockingServerArgs(TNonblockingServerTransport transport) {
      super(transport);
      transportFactory(new TFramedTransport.Factory());
    }
  }

  /**
   * The maximum amount of memory we will allocate to client IO buffers at a
   * time. Without this limit, the server will gladly allocate client buffers
   * right into an out of memory exception, rather than waiting.
   */
  private final long MAX_READ_BUFFER_BYTES;

  /**
   * How many bytes are currently allocated to read buffers.
   */
  private final AtomicLong readBufferBytesAllocated = new AtomicLong(0);

  public AbstractNonblockingServer(AbstractNonblockingServerArgs args) {
    super(args);
    MAX_READ_BUFFER_BYTES = args.maxReadBufferBytes;
  }

  /**
   * Begin accepting connections and processing invocations.
   */
  public void serve() {
    // start any IO threads
    if (!startThreads()) {
      return;
    }

    // start listening, or exit
    if (!startListening()) {
      return;
    }

    setServing(true);

    // this will block while we serve
    waitForShutdown();

    setServing(false);

    // do a little cleanup
    stopListening();
  }

  /**
   * Starts any threads required for serving.
   * 
   * @return true if everything went ok, false if threads could not be started.
   */
  protected abstract boolean startThreads();

  /**
   * A method that will block until when threads handling the serving have been
   * shut down.
   */
  protected abstract void waitForShutdown();

  /**
   * Have the server transport start accepting connections.
   * 
   * @return true if we started listening successfully, false if something went
   *         wrong.
   */
  protected boolean startListening() {
    try {
      serverTransport_.listen();
      return true;
    } catch (TTransportException ttx) {
      LOGGER.error("Failed to start listening on server socket!", ttx);
      return false;
    }
  }

  /**
   * Stop listening for connections.
   */
  protected void stopListening() {
    serverTransport_.close();
  }

  /**
   * Perform an invocation. This method could behave several different ways -
   * invoke immediately inline, queue for separate execution, etc.
   * 
   * @return true if invocation was successfully requested, which is not a
   *         guarantee that invocation has completed. False if the request
   *         failed.
   */
  protected abstract void requestInvoke( TNonblockingTransport transport, ByteBuffer frameBuffer);


}
