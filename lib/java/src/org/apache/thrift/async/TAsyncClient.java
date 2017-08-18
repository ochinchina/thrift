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
import java.util.concurrent.*;

import org.apache.thrift.TApplicationException;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TMessage;
import org.apache.thrift.protocol.TMessageType;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.TMemoryBuffer;
import org.apache.thrift.transport.TMemoryInputTransport;
import org.apache.thrift.transport.TNonblockingMessageListener;
import org.apache.thrift.transport.TNonblockingTransport;

public abstract class TAsyncClient {
  protected final TProtocolFactory ___protocolFactory;
  protected final TNonblockingTransport ___transport;
  //timeout in milliseconds
  private long ___timeout;
  private ConcurrentHashMap< Integer, TAsyncMethodCall > methodCalls = new ConcurrentHashMap<Integer, TAsyncMethodCall>(); 
  private volatile ExecutorService responseProcThreadPool = null;
  private static ScheduledExecutorService methodTimeoutTimer = Executors.newScheduledThreadPool(2);
  private MethodTimeoutManager methodTimeoutMgr = new MethodTimeoutManager();

  public TAsyncClient(TProtocolFactory protocolFactory, TNonblockingTransport transport) {
   this(protocolFactory,  transport, 0);
  }

  public TAsyncClient(TProtocolFactory protocolFactory, TNonblockingTransport transport, long timeout) {
    this.___protocolFactory = protocolFactory;
    this.___transport = transport;
    this.___timeout = timeout;
    try {
    	this.___transport.setMessageListener(createMessageListener());
	    this.___transport.start();
    }catch( Exception ex ) {    	
    }
  }
  
 
  private TNonblockingMessageListener createMessageListener() {
	  return new TNonblockingMessageListener() {
			
			@Override
			public void msgReceived( final TNonblockingTransport transport, final ByteBuffer msgBuf) {
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
						methodTimeoutMgr.stopTimeoutTimer(method);
						method.processResponse( msgBuf );
					} 
				}catch( Exception ex ) {
				}
				
			}

			@Override
			public void exception( final TNonblockingTransport transport,final Exception ex ) {
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
				HashMap<Integer, TAsyncMethodCall> tmp = new HashMap<Integer, TAsyncMethodCall>( TAsyncClient.this.methodCalls );
				
				TAsyncClient.this.methodCalls.clear();
				
				for( int id: tmp.keySet()) {
					TAsyncMethodCall method = tmp.get( id );
					methodTimeoutMgr.stopTimeoutTimer(method);
					TAsyncClient.this.processException( method, new TApplicationException( "connection lost"));
				}
			}
			
	    };
  }

public TProtocolFactory getProtocolFactory() {
    return ___protocolFactory;
  }

  public long getTimeout() {
    return ___timeout;
  }
  
  public void call( final TAsyncMethodCall method ) throws TException {
	  if( !method.isOneWay() ) {
		  methodCalls.put( method.getSequenceId(), method );
		  //startTimeoutTimer( method );
	  }
	  
	  ByteBuffer request = method.getRequest();
	  
	    try {
			___transport.asyncWrite( request, new TNonblockingTransport.AsyncWriteCallback() {

				@Override
				public void writeFinished(boolean success) {
					if( success) {
						if( !method.isOneWay() ) {
							startTimeoutTimer( method );	
						}
					} else {
						processException( method, new TApplicationException( "fail to send the request"));
					}
					
				}
				
			});
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
			methodTimeoutMgr.startTimeoutTimer(method);			
		}
		
	}
  
  private Runnable createMethodTimerTask( final TAsyncMethodCall method ) {
  	return () -> {
		methodTimeoutMgr.removeTimeoutTimer( method );
		processException( method, new TApplicationException("method is timeout") );
	};
  }
  
  private void stopTimeoutTimer( final TAsyncMethodCall method ) {
	  methodTimeoutMgr.stopTimeoutTimer(method);
  }


	private void processException( final TAsyncMethodCall method, final TApplicationException ex ) {
		if( getMethod( method.getSequenceId() ) == method ) {
			stopTimeoutTimer( method );
			if( responseProcThreadPool == null ) {
				try {
					method.processError(ex);
				}catch( Exception e ) {					
				}
			} else {
				responseProcThreadPool.submit( new Runnable() {
					@Override public void run() {
						 try {
							 method.processError(ex);								
						 }catch( Exception e ) {							 
						 }
					}
					
				});
			}
		}
	}
	
	private class MethodTimeoutManager {
		private ConcurrentHashMap< TAsyncMethodCall, ScheduledFuture<?> > methodTimerTasks = new ConcurrentHashMap<>();
		
		void startTimeoutTimer( final TAsyncMethodCall method ) {
			Runnable timerTask = createMethodTimerTask(method);
			
			ScheduledFuture<?> f = methodTimeoutTimer.schedule(timerTask, ___timeout, TimeUnit.MILLISECONDS);
			methodTimerTasks.put( method, f );

		}
		
		void stopTimeoutTimer( final TAsyncMethodCall method ) {
			ScheduledFuture<?> timerTask = methodTimerTasks.remove( method );
			if( timerTask != null ) timerTask.cancel(false);
		}
		
		void removeTimeoutTimer(  TAsyncMethodCall method ) {
			methodTimerTasks.remove( method );
		}
	}
  
}
