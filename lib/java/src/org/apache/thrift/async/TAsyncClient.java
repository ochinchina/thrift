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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;

import org.apache.thrift.TApplicationException;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TMessage;
import org.apache.thrift.protocol.TMessageType;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.TMemoryBuffer;
import org.apache.thrift.transport.TMemoryInputTransport;
import org.apache.thrift.transport.TNonblockingTransport;

public abstract class TAsyncClient {
  protected final TProtocolFactory ___protocolFactory;
  protected final TNonblockingTransport ___transport;
  //timeout in milliseconds
  private long ___timeout;
  private ConcurrentHashMap< Integer, TAsyncMethodCall > methodCalls = new ConcurrentHashMap<Integer, TAsyncMethodCall>(); 
  private ExecutorService responseProcThreadPool = null;
  private static Timer methodTimeoutTimer = new Timer();
  

  public TAsyncClient(TProtocolFactory protocolFactory, TNonblockingTransport transport) {
    this(protocolFactory,  transport, 0);
  }

  public TAsyncClient(TProtocolFactory protocolFactory, TNonblockingTransport transport, long timeout) {
    this.___protocolFactory = protocolFactory;
    this.___transport = transport;
    this.___timeout = timeout;
    try {
	    this.___transport.startAsyncRead( new TNonblockingTransport.MessageListener() {
			
			@Override
			public void msgReceived( final ByteBuffer msgBuf) {
				if( responseProcThreadPool == null ) {
					processReceivedMessage( msgBuf );
				} else {
					responseProcThreadPool.submit( new Runnable(){
						@Override
						public void run() {
							processReceivedMessage( msgBuf );
						}						
					});
				}
				
			}

			private void processReceivedMessage(ByteBuffer msgBuf) {
				try {
					int seqId = getSeqId( msgBuf );
					TAsyncMethodCall method = getMethod( seqId );
					if( method != null ) {
						method.processResponse( msgBuf );
					} 
				}catch( Exception ex ) {
					ex.printStackTrace();
				}
				
			}

			@Override
			public void exception( final Exception ex ) {
				if( responseProcThreadPool == null ) {
					processException( ex );
				} else {
					responseProcThreadPool.submit( new Runnable(){
						@Override
						public void run() {
							processException( ex );
						}						
					});
				}
			}

			private void processException(Exception ex) {
				HashMap<Integer, TAsyncMethodCall> tmp = new HashMap<Integer, TAsyncMethodCall>( methodCalls );
				
				methodCalls.clear();
				
				for( int id: tmp.keySet()) {
					TAsyncMethodCall method = tmp.get( id );
					handleException(method, ex );
				}
			}
			
	    });
    }catch( Exception ex ) {    	
    }
  }
  
 

public TProtocolFactory getProtocolFactory() {
    return ___protocolFactory;
  }

  public long getTimeout() {
    return ___timeout;
  }
  
  public void call( TAsyncMethodCall method ) throws TException {
	  if( !method.isOneWay() ) {
		  methodCalls.put( method.getSequenceId(), method );
		  startTimeoutTimer( method );
	  }
	  
	  ByteBuffer request = method.getRequest();
	  
	    try {
			___transport.asyncWrite( request );
		} catch (IOException e) {
			throw new TException( "fail to send the request", e );
		}
  }



public boolean hasTimeout() {
    return ___timeout > 0;
  }

  public void setTimeout(long timeout) {
    this.___timeout = timeout;
  }
  
  public void setResponseProcThreadPool( ExecutorService responseProcThreadPool ) {
	  this.responseProcThreadPool = responseProcThreadPool;
  }
  
  private void handleException( TAsyncMethodCall method, Exception ex ) {
	  /*TMemoryBuffer memoryBuffer = new TMemoryBuffer(128);
		TProtocol prot = ___protocolFactory.getProtocol(memoryBuffer);
		try {
			prot.writeMessageBegin( new TMessage( method.getName(), TMessageType.EXCEPTION, method.getSequenceId()));
			TApplicationException e = new TApplicationException( TApplicationException.INTERNAL_ERROR, ex.getMessage() );
			e.write(prot);
			prot.writeMessageEnd();
			ByteBuffer frameBuf = ByteBuffer.allocate( memoryBuffer.length() );
			frameBuf.put( memoryBuffer.getArray(), memoryBuffer.getBufferPosition(), memoryBuffer.length() );
			method.processResponse( frameBuf );
		} catch (TException e) {
			e.printStackTrace();
		}*/
	  method.processError(ex);
  }

  private TAsyncMethodCall getMethod(int seqId) {
	  
	return methodCalls.remove( seqId );
}

  private int getSeqId(ByteBuffer responseBuf) throws TException {
	TProtocol protocol = ___protocolFactory.getProtocol( new TMemoryInputTransport( responseBuf.array() ));
	TMessage msg = protocol.readMessageBegin();
	return msg.seqid;
	
}

  private void startTimeoutTimer( final TAsyncMethodCall method) {
		if( hasTimeout() ) {
			methodTimeoutTimer.schedule( new TimerTask() {

				@Override
				public void run() {
					processTimeout(method);
				}
			}, ___timeout );
		}
		
	}

	private void processTimeout(final TAsyncMethodCall method) {
		if( getMethod( method.getSequenceId() ) != null ) {
			if( responseProcThreadPool == null ) {
				handleException( method, new TApplicationException("method is timeout"));
			} else {
				responseProcThreadPool.submit( new Runnable() {
					@Override
					public void run() {
						handleException( method, new TApplicationException("method is timeout"));								
					}
					
				});
			}
		}
	}
  
}
