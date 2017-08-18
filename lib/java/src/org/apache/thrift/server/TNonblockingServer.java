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

import org.apache.thrift.TByteArrayOutputStream;
import org.apache.thrift.TException;
import org.apache.thrift.TProcessor.CompleteCallback;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TIOStreamTransport;
import org.apache.thrift.transport.TMemoryInputTransport;
import org.apache.thrift.transport.TNonblockingMessageListener;
import org.apache.thrift.transport.TNonblockingServerTransport;
import org.apache.thrift.transport.TNonblockingTransport;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.util.Iterator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * A nonblocking TServer implementation. This allows for fairness amongst all
 * connected clients in terms of invocations.
 *
 * This server is inherently single-threaded. If you want a limited thread pool
 * coupled with invocation-fairness, see THsHaServer.
 *
 * To use this server, you MUST use a TFramedTransport at the outermost
 * transport, otherwise this server will be unable to determine when a whole
 * method call has been read off the wire. Clients must also use TFramedTransport.
 */
public class TNonblockingServer extends AbstractNonblockingServer {

  public static class Args extends AbstractNonblockingServerArgs<Args> {
    public Args(TNonblockingServerTransport transport) {
      super(transport);
    }
  }

  // Flag for stopping the server
  private volatile boolean stopped_ = true;  
  private ExecutorService threadPool_ = null;
  


  public TNonblockingServer(AbstractNonblockingServerArgs args,  ExecutorService threadPool ) {
    super(args);
    this.threadPool_ = threadPool;
  }


  /**
   * Start the selector thread to deal with accepts and client messages.
   *
   * @return true if everything went ok, false if we couldn't start for some
   * reason.
   */
  @Override
  protected boolean startThreads() {
	  ((TNonblockingServerTransport)serverTransport_).accept( new TNonblockingServerTransport.TransportAcceptCallback() {

		@Override
		public void accepted(TNonblockingTransport transport) {
			transport.setMessageListener(createMessageListener());
			try {
				transport.start();
			} catch (IOException e) {
			}
		}
		  
	  });		
	this.stopped_ = false;
    return true;
  }
  
  private TNonblockingMessageListener createMessageListener( ) {
	  return new TNonblockingMessageListener() {

		@Override
		public void msgReceived( final TNonblockingTransport transport, final ByteBuffer msgBuf) {
			if( threadPool_ == null ) {
				requestInvoke( transport, msgBuf );
			} else {
				threadPool_.submit( new Runnable() {
					@Override
					public void run() {
						requestInvoke( transport, msgBuf );
					}
				});
			}
			
		}

		@Override
		public void exception(final TNonblockingTransport transport, Exception ex) {
			// TODO Auto-generated method stub
			
		}
		  
	  };
  }

  @Override
  protected void waitForShutdown() {
    while( !stopped_ ) {
    	try {
			Thread.sleep( 1000 );
		} catch (InterruptedException e) {
		}
    }
  }


  /**
   * Stop serving and shut everything down.
   */
  @Override
  public void stop() {
    stopped_ = true;
    ((TNonblockingServerTransport)serverTransport_).close();
  }

  /**
   * Perform an invocation. This method could behave several different ways
   * - invoke immediately inline, queue for separate execution, etc.
   */
  @Override
  protected void requestInvoke( final TNonblockingTransport transport, ByteBuffer frameBuffer) {
	  TTransport inTrans = new TMemoryInputTransport( frameBuffer.array());
      TProtocol inProt = inputProtocolFactory_.getProtocol(inTrans);
      final TByteArrayOutputStream response = new TByteArrayOutputStream();
      

      try {
    	  TProtocol outProt = outputProtocolFactory_.getProtocol( outputTransportFactory_.getTransport(new TIOStreamTransport(response )) );
        processorFactory_.getProcessor(inTrans).process(inProt, outProt, new CompleteCallback() {

			@Override
			public void completed() {
				 try {
					transport.asyncWrite( ByteBuffer.wrap( response.toByteArray() ), new TNonblockingTransport.AsyncWriteCallback() {
							
							@Override
							public void writeFinished(boolean success) {
								if( !success ) LOGGER.error( "fail to write the message to client");
							}
						} );
				} catch (IOException e) {
			        LOGGER.error("Unexpected throwable while invoking!", e);
				} 
				
			}
        	
        });       
      } catch (Throwable t) {
        LOGGER.error("Unexpected throwable while invoking!", t);
      }
  }


public boolean isStopped() {
	  return stopped_;
  }

  
}
