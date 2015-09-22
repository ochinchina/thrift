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
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Transport for use with async client.
 */
public class TNonblockingSocket extends TNonblockingTransport {

  private static final Logger LOGGER = LoggerFactory.getLogger(TNonblockingSocket.class.getName());

  /**
   * Host and port if passed in, used for lazy non-blocking connect.
   */
  private final SocketAddress socketAddress_;

  private final SocketChannel socketChannel_;

  private TSelector selector_;
  private ByteBuffer frameSizeBuffer = ByteBuffer.allocate(4);
  private ByteBuffer frameBuffer = null;

  

  public TNonblockingSocket(String host, int port, TSelector selector) throws IOException {
    this(host, port, 0, selector );
  }

  /**
   * Create a new nonblocking socket transport that will be connected to host:port.
   * @param host
   * @param port
   * @throws TTransportException
   * @throws IOException
   */
  public TNonblockingSocket(String host, int port, int timeout, TSelector selector ) throws IOException {
    this(SocketChannel.open(), timeout, new InetSocketAddress(host, port), selector );
  }

  /**
   * Constructor that takes an already created socket.
   *
   * @param socketChannel Already created SocketChannel object
   * @throws IOException if there is an error setting up the streams
   */
  public TNonblockingSocket(SocketChannel socketChannel, TSelector selector ) throws IOException {
    this(socketChannel, 0, null, selector );
    if (!socketChannel.isConnected()) throw new IOException("Socket must already be connected");
  }

  private TNonblockingSocket(SocketChannel socketChannel, int timeout, SocketAddress socketAddress, TSelector selector )
      throws IOException {
    socketChannel_ = socketChannel;
    socketAddress_ = socketAddress;


    // set options
    Socket socket = socketChannel.socket();
    socket.setSoLinger(false, 0);
    socket.setTcpNoDelay(true);
    setTimeout(timeout);
    if( socketAddress_ != null ) {
    	socket.connect( socketAddress_ );
    }
    // make it a nonblocking channel
    socketChannel.configureBlocking(false);
    this.selector_ = selector;
  }

 
  /**
   * Sets the socket timeout, although this implementation never uses blocking operations so it is unused.
   *
   * @param timeout Milliseconds timeout
   */
  public void setTimeout(int timeout) {
    try {
      socketChannel_.socket().setSoTimeout(timeout);
    } catch (SocketException sx) {
      LOGGER.warn("Could not set socket timeout.", sx);
    }
  }

  /**
   * Returns a reference to the underlying SocketChannel.
   */
  public SocketChannel getSocketChannel() {
    return socketChannel_;
  }

  /**
   * Checks whether the socket is connected.
   */
  public boolean isOpen() {
    // isConnected() does not return false after close(), but isOpen() does
    return socketChannel_.isOpen() && socketChannel_.isConnected();
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
    return socketChannel_.read(buffer);
  }


  /**
   * Reads from the underlying input stream if not null.
   */
  public int read(byte[] buf, int off, int len) throws TTransportException {
    if ((socketChannel_.validOps() & SelectionKey.OP_READ) != SelectionKey.OP_READ) {
      throw new TTransportException(TTransportException.NOT_OPEN,
        "Cannot read from write-only socket channel");
    }
    try {
      return socketChannel_.read(ByteBuffer.wrap(buf, off, len));
    } catch (IOException iox) {
      throw new TTransportException(TTransportException.UNKNOWN, iox);
    }
  }

  /**
   * Perform a nonblocking write of the data in buffer;
   */
  public int write(ByteBuffer buffer) throws IOException {
    return socketChannel_.write(buffer);
  }

  /**
   * Writes to the underlying output stream if not null.
   */
  public void write(byte[] buf, int off, int len) throws TTransportException {
    if ((socketChannel_.validOps() & SelectionKey.OP_WRITE) != SelectionKey.OP_WRITE) {
      throw new TTransportException(TTransportException.NOT_OPEN,
        "Cannot write to write-only socket channel");
    }
    try {
      socketChannel_.write(ByteBuffer.wrap(buf, off, len));
    } catch (IOException iox) {
      throw new TTransportException(TTransportException.UNKNOWN, iox);
    }
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
    try {
      socketChannel_.close();
    } catch (IOException iox) {
      LOGGER.warn("Could not close socket.", iox);
    }
  }



	@Override
	public void startAsyncRead( final MessageListener listener) throws IOException {
		selector_.register( socketChannel_, SelectionKey.OP_READ, new TSelector.ChannelOperator() {
			
			@Override
			public void doOperation() {
				ByteBuffer frameBuf = readFrame();
				if( frameBuf != null ) {
					listener.msgReceived( frameBuf );
				}
				
			}
			
		}, -1 );
		
	}
	
	

	protected ByteBuffer readFrame() {
			try {
				if( frameSizeBuffer.remaining() != 0 ) {
					socketChannel_.read( frameSizeBuffer );
					if( frameSizeBuffer.remaining() == 0 ) {
						int n = TFramedTransport.decodeFrameSize(frameSizeBuffer.array());
						frameBuffer = ByteBuffer.allocate( n );
						socketChannel_.read( frameBuffer );
					}
				} else {
					socketChannel_.read( frameBuffer );
				}
				
				if( frameSizeBuffer.remaining() == 0 && frameBuffer.remaining() == 0 ) {
					//frameSizeBuffer = ByteBuffer.allocate( 4 );
					frameSizeBuffer.limit( frameSizeBuffer.capacity() );
					frameSizeBuffer.position( 0 );
					return frameBuffer;
				}
			} catch (IOException e) {
				selector_.cancel( socketChannel_, SelectionKey.OP_READ);
				startConnect();
				//e.printStackTrace();
			}
		
		return null;
		
		
	}

	private void startConnect() {
		selector_.register( socketChannel_, SelectionKey.OP_CONNECT, new TSelector.ChannelOperator() {
			
			@Override
			public void doOperation() {
				try {
					socketChannel_.finishConnect();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}, -1 );
		
		try {
			socketChannel_.connect( socketAddress_ );
		} catch (IOException e) {
			e.printStackTrace();
		}		
	}
	
	@Override
	public void asyncWrite( final ByteBuffer buffer) throws IOException {
		
		selector_.register( socketChannel_, SelectionKey.OP_WRITE, new TSelector.ChannelOperator() {			
			@Override
			public void doOperation() {
				try {
					socketChannel_.write( buffer );
				} catch (IOException e) {
					e.printStackTrace();
				}				
			}
			
		}, 1 );
		
	}

}
