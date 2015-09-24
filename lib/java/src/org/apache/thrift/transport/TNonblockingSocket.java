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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Transport for use with async client.
 */
public class TNonblockingSocket extends TNonblockingTransport {

  private static final Logger LOGGER = LoggerFactory.getLogger(TNonblockingSocket.class.getName());

  private AsyncChannel asyncChannel_;
  
  private ByteBuffer frameSizeBuffer = ByteBuffer.allocate(4);
  private ByteBuffer frameBuffer = null;
  private static final Timer connectTimer = new Timer();

  

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
    this(SocketChannel.open(), timeout, new InetSocketAddress(host, port) );
  }

  /**
   * Constructor that takes an already created socket.
   *
   * @param socketChannel Already created SocketChannel object
   * @throws IOException if there is an error setting up the streams
   */
  public TNonblockingSocket(SocketChannel socketChannel) throws IOException {
    this(socketChannel, 0, null );
    if (!socketChannel.isConnected()) throw new IOException("Socket must already be connected");
  }

  private TNonblockingSocket(SocketChannel socketChannel, int timeout, SocketAddress socketAddress )
      throws IOException {
	if( socketChannel != null && socketChannel.isConnected() ) {
		this.asyncChannel_ = new AsyncChannel( TSelector.getDefaultInstance(), socketChannel, timeout );
	} else {
		this.asyncChannel_ = new AsyncChannel( TSelector.getDefaultInstance(), socketAddress, timeout );
	}    
  }

 
 @Override
 public void start() {
	 this.asyncChannel_.start();
 }
 
 public void setMessageListener( TNonblockingMessageListener listener ) {
	 this.asyncChannel_.setMessageListener( listener );
 }
  /**
   * Sets the socket timeout, although this implementation never uses blocking operations so it is unused.
   *
   * @param timeout Milliseconds timeout
   */
  public void setTimeout(int timeout) {
	  asyncChannel_.setTimeout( timeout );
  }

  /**
   * Returns a reference to the underlying SocketChannel.
   */
  public SocketChannel getSocketChannel() {
    return asyncChannel_.getSocketChannel();
  }

  /**
   * Checks whether the socket is connected.
   */
  public boolean isOpen() {
    // isConnected() does not return false after close(), but isOpen() does
	  return asyncChannel_.isConnected();
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
	  asyncChannel_.close();
  }



	
		

	protected ByteBuffer readFrame( SocketChannel sockChannel ) throws IOException  {
				if( frameSizeBuffer.remaining() != 0 ) {
					sockChannel.read( frameSizeBuffer );
					if( frameSizeBuffer.remaining() == 0 ) {
						int n = TFramedTransport.decodeFrameSize(frameSizeBuffer.array());
						frameBuffer = ByteBuffer.allocate( n );
						sockChannel.read( frameBuffer );
					}
				} else {
					sockChannel.read( frameBuffer );
				}
				
				if( frameSizeBuffer.remaining() == 0 && frameBuffer.remaining() == 0 ) {
					//frameSizeBuffer = ByteBuffer.allocate( 4 );
					frameSizeBuffer.limit( frameSizeBuffer.capacity() );
					frameSizeBuffer.position( 0 );
					return frameBuffer;
				}
				return null;
		
		
	}
	
	
	
	
	@Override
	public void asyncWrite( final ByteBuffer buffer, final AsyncWriteListener listener ) throws IOException {
		asyncChannel_.asyncWrite(buffer, listener);		
	}
	
	private class ProxyMessageListener implements TNonblockingMessageListener {
		
		private TNonblockingMessageListener listener_ = null;
		private volatile boolean exceptionRaised = false;
		
		ProxyMessageListener(TNonblockingMessageListener listener ) {
			listener_ = listener;
		}
		
		
		@Override
		public void msgReceived(TNonblockingTransport transport, ByteBuffer msgBuf) {
			exceptionRaised = false;
			listener_.msgReceived(transport, msgBuf);
		}

		@Override
		public void exception(TNonblockingTransport transport, Exception ex) {
			if( !exceptionRaised) {
				exceptionRaised = true;
				listener_.exception(transport, ex);
			}
		}
		
	}
	
	
	private class AsyncChannel {
		private TSelector selector_;
		private volatile boolean stop_ = false;
		private volatile SocketChannel socketChannel_;
		private SocketAddress socketAddress_;
		private int timeout_;
		private TNonblockingMessageListener listener = null;
		
		AsyncChannel( TSelector selector, SocketChannel socketChannel, int timeout ) {
			this.selector_ = selector;
			this.socketChannel_ = socketChannel;
			this.timeout_ = timeout;
			setSocketOptions();
			
			
		}
		
		public void setMessageListener(TNonblockingMessageListener listener) {
			this.listener = new ProxyMessageListener( listener );
			
		}

		public void start() {
			if( isConnected() ) {
				doRead();
			}else {
				connectLater( 10 );
			}
		}

		void close() {
			try {
				stop_ = true;
				socketChannel_.close();
			}catch( Exception ex ) {				
			}
			
		}

		SocketChannel getSocketChannel() {
			return socketChannel_;
		}

		void setTimeout(int timeout) {
			this.timeout_ = timeout;
			try {
				socketChannel_.socket().setSoTimeout(timeout);
			}catch( Exception ex ) {
				
			}
		}

		AsyncChannel( TSelector selector, SocketAddress socketAddress, int timeout ) throws IOException {
			this.selector_ = selector;
			this.socketAddress_ = socketAddress;
			this.timeout_ = timeout;
			this.listener = new ProxyMessageListener( listener );
			
		}
		
		private void setSocketOptions() {
			try {
				socketChannel_.configureBlocking( false );
				socketChannel_.socket().setSoLinger(false, 0);
				socketChannel_.socket().setTcpNoDelay(true);
				socketChannel_.socket().setSoTimeout(timeout_);
			} catch (IOException e) {
			}
			
		}
		
		
		void doRead() {
			
			if( stop_ ) {
				listener.exception( TNonblockingSocket.this, new Exception("already stoped") );
				return;
			}
			
			selector_.register( socketChannel_, SelectionKey.OP_READ, new TSelector.ChannelOperator() {
			
					
					@Override
					public void doOperation()  {
						try {
							ByteBuffer frameBuf = readFrame(socketChannel_);
							if( frameBuf != null ) {
								listener.msgReceived( TNonblockingSocket.this, frameBuf );
							}							
							doRead();
						}catch( Exception ex ) {
							listener.exception( TNonblockingSocket.this, new Exception("connection lost") );
							connectLater( 100 );
						}
					}
					
			}, 1 );
		}
		
		void asyncWrite( final ByteBuffer buffer, final AsyncWriteListener writeListener )  {
			if( stop_ || !isConnected() ) {
				writeListener.writeFinished(false);
				return;
			}
			
			selector_.register( socketChannel_, SelectionKey.OP_WRITE, new TSelector.ChannelOperator() {			
				@Override
				public void doOperation() {
					try {
						socketChannel_.write( buffer );
						writeListener.writeFinished(true);
					} catch (IOException e) {
						LOGGER.error( "fail to write to the server", e);
						writeListener.writeFinished(false);
					}				
				}
				
			}, 1 );
		}
		
		private void connectLater( int delayMillis ) {
			if( stop_ ) {
				return;
			}
			
			if( delayMillis <= 0 ) {
				doConnect();
			} else {
				connectTimer.schedule( new TimerTask(){
	
					@Override
					public void run() {
						doConnect();				
					}
					
				}, delayMillis);
			}
		}
		
		
		private void doConnect() {
			
			
			if( socketAddress_ == null ) {
				return;
			}
			
			try {
				socketChannel_.close();
			}catch( Exception ex ) {
				
			}
			try {
				socketChannel_ = SocketChannel.open();
				setSocketOptions();
				selector_.register( socketChannel_, SelectionKey.OP_CONNECT, new TSelector.ChannelOperator() {
					
					@Override
					public void doOperation() {
						try {
							socketChannel_.finishConnect();
						}catch( Exception ex ) {
							ex.printStackTrace();
						}
						if( isConnected() ) {
							doRead();
						} else {
							connectLater(100);
						}
					}
				}, 1 );
				
				try {
					if( socketChannel_.connect( socketAddress_ )  ) {
						doRead();
					} 
				} catch (IOException e) {
					connectLater( 100 );
				}		
				
			} catch (IOException e1) {
				connectLater( 100 );
			}
		}
		
		private boolean isConnected() {
			return socketChannel_ != null && socketChannel_.isOpen() && socketChannel_.isConnected();
		}
	}

}
