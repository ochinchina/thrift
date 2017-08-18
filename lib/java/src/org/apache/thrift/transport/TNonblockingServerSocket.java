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
import java.net.ServerSocket;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Wrapper around ServerSocketChannel
 */
public class TNonblockingServerSocket extends TNonblockingServerTransport {
  private static final Logger LOGGER = LoggerFactory.getLogger(TNonblockingServerSocket.class.getName());


  /**
   * Timeout for client sockets from accept
   */
  private int clientTimeout_ = 0;
  private TSelector selector = TSelector.getDefaultInstance();
  private TransportAcceptCallback acceptCallback;
  private InetSocketAddress bindAddr;
  private ConcurrentHashMap<SocketChannel, ClientTransport> clients = new ConcurrentHashMap<>();

  /**
   * Creates just a port listening server socket
   */
  public TNonblockingServerSocket(int port) throws TTransportException {
    this(port, 0);
  }

    public TNonblockingServerSocket(String bindAddr, int port) throws TTransportException {
        this(bindAddr, port, 0);
    }

  /**
   * Creates just a port listening server socket
   */
  public TNonblockingServerSocket(int port, int clientTimeout) throws TTransportException {
    this(new InetSocketAddress(port), clientTimeout);
  }

    public TNonblockingServerSocket(String bindAddr, int port, int clientTimeout) throws TTransportException {
        this(new InetSocketAddress(bindAddr, port), clientTimeout);
    }

  public TNonblockingServerSocket(InetSocketAddress bindAddr) throws TTransportException {
    this(bindAddr, 0);
  }

  public TNonblockingServerSocket(InetSocketAddress bindAddr, int clientTimeout) throws TTransportException {
    this.bindAddr = bindAddr;
    clientTimeout_ = clientTimeout;

  }

  public void listen() throws TTransportException {

  }

  protected TTransport acceptImpl() throws TTransportException {
    return null;
  }

 

  public void close() {

  }

  public void interrupt() {
    // The thread-safeness of this is dubious, but Java documentation suggests
    // that it is safe to do this from a different thread context
    close();
  }

  @Override
  public void accept( final TransportAcceptCallback acceptCallback) {
      this.acceptCallback = acceptCallback;
    TSelector.ChannelHandler channelHandler = new TSelector.ChannelHandler() {

      @Override
      public void handleConnect(SocketChannel channel) {
        if( channel.isConnected()) {
            ClientTransport client = new ClientTransport( channel );
            clients.put( channel, client );
            acceptCallback.accepted( client);
        }
      }

      @Override
      public void handleRead(SocketChannel channel) throws IOException {
          ClientTransport clientTransport = clients.get(channel);
          if( clientTransport != null ) {
              clientTransport.doRead();
          }

      }

      @Override
      public void handleException(SocketChannel channel) {
          clients.remove( channel );
      }
    };
    selector.listen( bindAddr.getHostName(), bindAddr.getPort(), channelHandler );

  }



    private class ClientTransport extends TNonblockingTransport {

        private SocketChannel channel;
        private StoreForwardMessageListener listener = new StoreForwardMessageListener( this );
        private TFrameReader frameReader;

        public ClientTransport( SocketChannel channel ) {
            this.channel = channel;
            this.frameReader = new TFrameReader( channel);
        }
        @Override
        public void setMessageListener(TNonblockingMessageListener listener) {
            this.listener.setListener( listener);
        }

        @Override
        public void start() throws IOException {

        }

        @Override
        public void asyncWrite(ByteBuffer buffer, AsyncWriteCallback cb) throws IOException {
            selector.write( channel, buffer, cb );
        }

        @Override
        public boolean isOpen() {
            return this.channel.isConnected();
        }

        @Override
        public void open() throws TTransportException {

        }

        @Override
        public void close() {

        }

        @Override
        public int read(byte[] buf, int off, int len) throws TTransportException {
            return 0;
        }

        @Override
        public void write(byte[] buf, int off, int len) throws TTransportException {

        }

        public void doRead() throws IOException{
                ByteBuffer frame = frameReader.readFrom();
                if( frame != null ) {
                    this.listener.msgReceived( this, frame );
                }
        }


    }


}
