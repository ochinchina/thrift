package org.apache.thrift.transport;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.nio.channels.spi.SelectorProvider;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TSelector {
	private static final Logger LOGGER = LoggerFactory.getLogger(TSelector.class);
	private SelectThread selectThread;

	public static interface ChannelHandler {
	    void handleConnect( SocketChannel channel);
		void handleRead(  SocketChannel channel ) throws IOException;
		void handleException( SocketChannel channel );
	}


	private class WriteOperation {
	    ByteBuffer byteBuffer;
        TNonblockingTransport.AsyncWriteCallback writeCallback;
        public WriteOperation( ByteBuffer byteBuffer, TNonblockingTransport.AsyncWriteCallback writeCallback) {
            this.byteBuffer = byteBuffer;
            this.writeCallback = writeCallback;
        }
    }




    private ConcurrentHashMap<SocketChannel, ConcurrentLinkedQueue< WriteOperation>> writeOperations = new ConcurrentHashMap<>();
    private ConcurrentHashMap<SocketChannel, ChannelHandler > channelHandlers = new ConcurrentHashMap<>();
    private Selector selector;

	private static TSelector defInstance_ = new TSelector();


    public static TSelector newInstance()  {
        return new TSelector();
    }
	
	public static TSelector getDefaultInstance() {
		return defInstance_;
	}


	private TSelector() {
		try {
            selector = SelectorProvider.provider().openSelector();
			selectThread = new SelectThread();
			selectThread.start();
		} catch (IOException e) {
		    e.printStackTrace();
		}
	}

	public void connect( final String host, final int port, final ChannelHandler channelHandler ) throws IOException {
        LOGGER.info( "try to connect to " + host + ":" + port );
        SocketChannel channel = SocketChannel.open();
        try {
            channel.configureBlocking( false );
            channel.socket().setSoLinger(false, 60);
            channel.socket().setTcpNoDelay(true);
            channel.socket().setSoTimeout(5000);
            if( channel.connect( new InetSocketAddress( host, port )) ) {
                channelHandlers.put( channel, channelHandler);
                writeOperations.put( channel, new ConcurrentLinkedQueue<>());
                channelHandler.handleConnect( channel);
            } else {
                channel.register(selector, SelectionKey.OP_CONNECT, channelHandler );
                wakeupSelector();
            }
        } catch (IOException e) {
            LOGGER.error( "fail to connect to server " + host + ":" + port, e );
            channelHandler.handleConnect( channel);
        }
    }




    public void listen( String host, int port, final ChannelHandler channelHandler) {
        LOGGER.info( "start to listen on " + host + ":" + port );
        try {
            final ServerSocketChannel serverChannel = ServerSocketChannel.open();
            serverChannel.configureBlocking(false);
            serverChannel.bind( new InetSocketAddress( host, port), 128 );
            serverChannel.register(selector, SelectionKey.OP_ACCEPT, channelHandler );
            wakeupSelector();
        } catch (IOException e) {
            LOGGER.error( "fail to listen on " + host + ":" + port, e );
        }
    }



    public void write(SocketChannel channel, final ByteBuffer byteBuffer, TNonblockingTransport.AsyncWriteCallback cb ) {
        try {
            ConcurrentLinkedQueue<WriteOperation> ops = writeOperations.get(channel);
            if( ops != null ) {
                ops.add( new WriteOperation( byteBuffer, cb ));
                channel.register( selector, SelectionKey.OP_READ | SelectionKey.OP_WRITE);
                wakeupSelector();
            }
        } catch (ClosedChannelException e) {
            LOGGER.error( "fail to write data", e );
        }
    }

    private void handleWrite( SelectionKey key ) {
        SocketChannel channel = (SocketChannel)key.channel();
        ConcurrentLinkedQueue<WriteOperation> ops = writeOperations.get(channel );
        if( ops == null ) {
            return;
        }
        if( ops.isEmpty()) {
            key.interestOps( key.interestOps() & ~SelectionKey.OP_WRITE);
        } else {
            WriteOperation op = ops.peek();
            try {
                int n = channel.write(op.byteBuffer);
            }catch ( Exception ex ) {
                ex.printStackTrace();
                handleException(channel);
            }
            if( !op.byteBuffer.hasRemaining() ) {
                ops.poll();
                op.writeCallback.writeFinished( true );
            }
        }

    }

    private void handleRead( SelectionKey key ) {
        SocketChannel channel = (SocketChannel)key.channel();
        try {
            ChannelHandler handler = channelHandlers.get( channel);
            handler.handleRead( channel);
        } catch (IOException e) {
            handleException( channel);
            LOGGER.error( "fail to read data", e );
        }

    }

    private void handleException( SocketChannel channel) {
        try {
            channel.register(selector, 0 );
        } catch (ClosedChannelException e) {
            e.printStackTrace();
        }
        channelHandlers.remove( channel);
        writeOperations.remove( channel).forEach( wo -> wo.writeCallback.writeFinished(false));

    }


	private void wakeupSelector() {
        if( Thread.currentThread().getId() != selectThread.getId() ) {
            selector.wakeup();
        }
    }



	  private class SelectThread extends Thread {

		    public SelectThread() throws IOException {
		      this.setName("TSelector#SelectorThread " + this.getId());
		      // We don't want to hold up the JVM when shutting down
		      setDaemon(true);
		    }
		    

		    public void run() {
		      for (;;) {
				try {
				    selector.select( 1000 );
				    processSelectedKeys();
		        } catch (Exception exception) {
		          LOGGER.error("Ignoring uncaught exception in SelectThread", exception);
		        }
		      }
		    }

          private void processSelectedKeys() {
                Iterator<SelectionKey> iter = selector.selectedKeys().iterator();
                while( iter.hasNext()) {
                    SelectionKey key = iter.next();
                    iter.remove();
                    if( key.isAcceptable() ) {
                        handleAccept( key );
                    } else if( key.isConnectable()) {
                        handleConnect( key );
                    } else if( key.isReadable()) {
                        handleRead( key );
                    } else if( key.isWritable()) {
                        handleWrite( key );
                    } else if( !key.isValid() ) {
                        handleException( (SocketChannel) key.channel() );
                    }
                }


          }

          private void handleConnect(SelectionKey key) {
              ChannelHandler channelHandler = (ChannelHandler) key.attachment();
              SocketChannel sockChannel = (SocketChannel) key.channel();
              try {
                  sockChannel.finishConnect();
                  writeOperations.put( sockChannel, new ConcurrentLinkedQueue<>() );
                  channelHandlers.put( sockChannel, channelHandler);
                  sockChannel.register(selector, SelectionKey.OP_READ);
                  LOGGER.info( "success to connect to " + sockChannel);
              }catch ( Exception ex ) {
                  LOGGER.error( "fail to connect to server " + sockChannel, ex );
              }
              channelHandler.handleConnect( sockChannel);
          }

          private void handleAccept(SelectionKey key) {
              ServerSocketChannel serverChannel = (ServerSocketChannel) key.channel();
              try {
                  ChannelHandler channelHandler = (ChannelHandler)key.attachment();

                  SocketChannel sockChannel = serverChannel.accept();
                  sockChannel.configureBlocking(false);
                  sockChannel.socket().setKeepAlive(true);
                  sockChannel.socket().setSoLinger(true, 60);
                  writeOperations.put( sockChannel, new ConcurrentLinkedQueue<>() );
                  channelHandlers.put( sockChannel, channelHandler);
                  LOGGER.info( "success to accept a connection " + sockChannel);
                  sockChannel.register(selector, SelectionKey.OP_READ);
                  channelHandler.handleConnect( sockChannel);
              }catch (Exception ex ) {
                  LOGGER.error( "fail to accept to connection " + serverChannel, ex );
              }
          }




	  }


}

	
