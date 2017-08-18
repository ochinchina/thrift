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


package org.apache.thrift.transport;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Transport for use with async client.
 */
public class TNonblockingSocket extends TNonblockingTransport {

  private static final Logger LOGGER = LoggerFactory.getLogger(TNonblockingSocket.class.getName());


  private TFrameReader frameReader;
  private static final Timer connectTimer = new Timer();
  private TSelector selector = TSelector.getDefaultInstance();
  private String host;
  private int port;
    private TNonblockingMessageListener listener;
    private SocketChannel channel;
    private AtomicInteger connectTriedTimes = new AtomicInteger(0);


    public TNonblockingSocket(String host, int port ) throws IOException {
    this(host, port, 0);
  }

  /**
   * Create a new nonblocking socket transport that will be connected to host:port.
   * @param host
   * @param port
   * @throws TTransportException
   * @throws IOException
   */
  public TNonblockingSocket(String host, int port, int timeout ) throws IOException {
    this.host = host;
    this.port = port;
  }



 
 @Override
 public void start() {
     try {
         selector.connect( host, port, createChannelHandler() );
         while( connectTriedTimes.get() <= 0 ) {
             try { Thread.sleep(10); }catch (Exception ex ) {}
         }
     }catch (Exception ex ) {
         LOGGER.error( "Fail to make connect to " + host + ":" + port );
     }

 }

 private TSelector.ChannelHandler createChannelHandler() {
     return new TSelector.ChannelHandler() {
         @Override
         public void handleConnect(SocketChannel channel) {
             TNonblockingSocket.this.channel = channel;
             connectTriedTimes.incrementAndGet();
             if( channel.isConnected()) {
                 LOGGER.info( "Connect to " + channel + " Successfully");
                 frameReader = new TFrameReader(channel);
             } else {
                 LOGGER.error( "Fail to connect " + host + ":" + port );
                 connectLater( 5000 );
             }
         }

         @Override
         public void handleRead(SocketChannel channel) throws IOException{
                 ByteBuffer frame = readFrame();
                 if( frame != null ) {
                     listener.msgReceived( TNonblockingSocket.this, frame);
                 }
         }

         @Override
         public void handleException(SocketChannel channel) {
             LOGGER.info( "Connection to " + channel + " is lost");

             listener.exception(TNonblockingSocket.this, new RuntimeException("Fail to read data from " + channel) );
             connectLater( 5000 );
         }
     };
 }
 private void connectLater( int millis ) {
     connectTimer.schedule(new TimerTask() {
         @Override
         public void run() {
             try {
                 selector.connect( host, port, createChannelHandler() );
             }catch (Exception ex ) {
             }
         }
     }, millis);
 }
 public void setMessageListener( TNonblockingMessageListener listener ) {
	 this.listener = listener;

 }
  /**
   * Sets the socket timeout, although this implementation never uses blocking operations so it is unused.
   *
   * @param timeout Milliseconds timeout
   */
  public void setTimeout(int timeout) {

  }

  /**
   * Returns a reference to the underlying SocketChannel.
   */
  public SocketChannel getSocketChannel() {
    return channel;
  }

  /**
   * Checks whether the socket is connected.
   */
  public boolean isOpen() {
    // isConnected() does not return false after close(), but isOpen() does
	  return channel != null && channel.isConnected();
  }

  /**
   * Do not call, the implementation provides its own lazy non-blocking connect.
   */
  public void open() throws TTransportException {
    throw new RuntimeException("open() is not implemented for TNonblockingSocket");
  }

  /**
   * Perform a nonblocking read into buffer.
   */
  public int read(ByteBuffer buffer) throws IOException {
	  throw new IOException( "not support this operation");
  }


  /**
   * Reads from the underlying input stream if not null.
   */
  public int read(byte[] buf, int off, int len) throws TTransportException {
	  throw new TTransportException( "not support this operation");
  }

  /**
   * Perform a nonblocking write of the data in buffer;
   */
  public int write(ByteBuffer buffer) throws IOException {
	  throw new IOException( "not support this operation");
  }

  /**
   * Writes to the underlying output stream if not null.
   */
  public void write(byte[] buf, int off, int len) throws TTransportException {
	  throw new TTransportException( "not support this operation");
  }

  /**
   * Noop.
   */
  public void flush() throws TTransportException {
    // Not supported by SocketChannel.
  }

  /**
   * Closes the socket.
   */
  public void close() {
	  if( isOpen()) {
          try {
              this.channel.close();
          } catch (IOException e) {
          }
      }
  }



	
		

	protected ByteBuffer readFrame( ) throws IOException  {
      return frameReader.readFrom();
	}
	
	
	
	
	@Override
	public void asyncWrite( final ByteBuffer buffer, final AsyncWriteCallback listener ) throws IOException {
		selector.write( channel, buffer, listener);
	}
	


}
