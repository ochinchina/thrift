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
package org.apache.thrift.async;


import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.thrift.TApplicationException;
import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TMessage;
import org.apache.thrift.protocol.TMessageType;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TMemoryBuffer;
import org.apache.thrift.transport.TNonblockingTransport;

/**
 * Encapsulates an async method call
 * Need to generate:
 *   - private void write_args(TProtocol protocol)
 *   - public T getResult() throws <Exception_1>, <Exception_2>, ...
 * @param <T>
 */
public abstract class TAsyncMethodCall<T> {

  private static final int INITIAL_MEMORY_BUFFER_SIZE = 128;
  private static AtomicInteger sequenceIdCounter = new AtomicInteger(0);


  
  protected final TNonblockingTransport transport;
  private final TProtocolFactory protocolFactory;
  protected final TAsyncClient client;
  private final AsyncMethodCallback<T> callback;
  private final boolean isOneway;
  private int sequenceId;
  private ByteBuffer responseBuffer = null;
  private String name = "";
  private AtomicBoolean finished = new AtomicBoolean( false );
  
  private final static byte[] sizeBufferArray = new byte[4];

  private long startTime = System.currentTimeMillis();

  protected TAsyncMethodCall(TAsyncClient client, TProtocolFactory protocolFactory, TNonblockingTransport transport, AsyncMethodCallback<T> callback, boolean isOneway) {
    this.transport = transport;
    this.callback = callback;
    this.protocolFactory = protocolFactory;
    this.client = client;
    this.isOneway = isOneway;
    this.sequenceId = TAsyncMethodCall.sequenceIdCounter.getAndIncrement();
  }


  protected long getStartTime() {
    return startTime;
  }
  
  protected int getSequenceId() {
    return sequenceId;
  }

  public TAsyncClient getClient() {
    return client;
  }
  
  public boolean hasTimeout() {
    return client.hasTimeout();
  }
  
  public long getTimeoutTimestamp() {
    return client.getTimeout() + startTime;
  }

  protected abstract void write_args(TProtocol protocol) throws TException;
  
  protected ByteBuffer getRequest() throws TException {
	  TMemoryBuffer memoryBuffer = new TMemoryBuffer(INITIAL_MEMORY_BUFFER_SIZE);
	  memoryBuffer.write( sizeBufferArray );
	    TProtocol protocol = protocolFactory.getProtocol(memoryBuffer);
	    write_args(protocol);

	    int length = memoryBuffer.length();
	    byte[] b = memoryBuffer.getArray();

	    TFramedTransport.encodeFrameSize(length - 4 , b);
	    return ByteBuffer.wrap( b, 0, length );
  }
  
  protected void processError( Exception ex ) {
	  if( finished.compareAndSet(false, true)) {
		  callback.onError(ex);
	  }
  }
  protected void processResponse(ByteBuffer respBuffer) {
	  if( finished.compareAndSet(false, true)) {
		  this.responseBuffer = respBuffer;
		  callback.onComplete((T)this);
	  }
	}

  protected void getResult( TBase result ) throws TException {
      org.apache.thrift.transport.TMemoryInputTransport memoryTransport = new org.apache.thrift.transport.TMemoryInputTransport(responseBuffer.array());
      org.apache.thrift.protocol.TProtocol prot = protocolFactory.getProtocol(memoryTransport);

	  TMessage msg = prot.readMessageBegin();
	    if (msg.type == TMessageType.EXCEPTION) {
	      TApplicationException x = TApplicationException.read(prot);
	      prot.readMessageEnd();
	      throw x;
	    }
	    result.read(prot);
	    prot.readMessageEnd();

  }

	public boolean isOneWay() {
		return this.isOneway;
	}


	public String getName() {		
		return this.name;
	}
}
