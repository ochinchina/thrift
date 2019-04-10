package org.apache.thrift.transport;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.ByteToMessageDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TNonblockingNettySocket extends TNonblockingTransport {
    private static final Logger LOGGER = LoggerFactory.getLogger(TNonblockingNettySocket.class);

	private String host;
	private int port;
	private int timeout;
	private TNonblockingMessageListener listener;
	private Channel channel;
	private EventLoopGroup eventLoopGroup = new NioEventLoopGroup();
	private Bootstrap bootstrap = new Bootstrap();
	private static final ScheduledExecutorService timeoutService = Executors.newScheduledThreadPool( 1 );
	private AtomicInteger connectTriedTimes = new AtomicInteger(0);
	
	public TNonblockingNettySocket( String host, int port) {
		this( host, port, 0 );
	}
	
	public TNonblockingNettySocket( String host, int port, int timeout) {
		this.host = host;
		this.port = port;
		this.timeout = timeout;
		initNetwork();
	}
	
	private void initNetwork() {
		bootstrap.group(eventLoopGroup)
		.channel(NioSocketChannel.class)
		.option(ChannelOption.TCP_NODELAY, true)
		.option(ChannelOption.SO_KEEPALIVE, true)
		.handler(new ChannelInitializer<SocketChannel>() {

			@Override
			protected void initChannel(SocketChannel ch) throws Exception {
				ch.pipeline().addLast( new TNettyMessageDecoder(), new MessageHandler() );
			}			
		});
		
		
	}
	@Override
	public void setMessageListener(TNonblockingMessageListener listener) {
		this.listener = listener;
		
		
	}

	@Override
	public void start() throws IOException {
		doConnect();
		while( connectTriedTimes.get() == 0 ) {
		    try { Thread.sleep(10); } catch (Exception ex ) {}
        }
	}
	
	
	@Override
	public void asyncWrite( final ByteBuffer buffer, final AsyncWriteCallback cb) throws IOException {
		if( isOpen() ) {
		    final ByteBuf data = channel.alloc().buffer( buffer.remaining() ).writeBytes( buffer );
			channel.writeAndFlush( data ).addListener( new ChannelFutureListener() {

				@Override
				public void operationComplete(ChannelFuture cf ) throws Exception {
                    if( cf.isSuccess()) {
                        cb.writeFinished( true );
                    } else {
                        cb.writeFinished( false );
                        LOGGER.error( "Fai to aend a message to server " + cf.channel().remoteAddress() );
                    }
					cb.writeFinished( cf.isSuccess() );					
				}
				
			});			
		} else {
            LOGGER.error( "No message is send to server, because fail to make a connect to server " + host + ":" + port );
			cb.writeFinished( false );
		}
		
	}

	@Override
	public boolean isOpen() {
		// TODO Auto-generated method stub
		return channel != null && channel.isOpen();
	}

	@Override
	public void open() throws TTransportException {
		throw new TTransportException( "not support this operation");
		
	}

	@Override
	public void close() {
		if( isOpen() ) {
			channel.close();
		}
		
	}
	
	private void reconnect() {

        timeoutService.schedule(() -> doConnect(), 5, TimeUnit.SECONDS);
    }

    private void doConnect() {
        LOGGER.info( "start to connect to server " + host + ":" + port );
		bootstrap.connect( host, port ).addListener( new ChannelFutureListener() {

			@Override
			public void operationComplete(ChannelFuture cf ) throws Exception {
				if( cf.isSuccess() ) {
				    LOGGER.info( "Successful connect to " + cf.channel().remoteAddress());
					TNonblockingNettySocket.this.channel = cf.channel();
					cf.channel().closeFuture().addListener( new ChannelFutureListener() {
						@Override
						public void operationComplete(ChannelFuture arg0) throws Exception {
                            LOGGER.error( "Connection is lost to " + arg0.channel().remoteAddress());
							reconnect();
						}
						
					});
				} else {//reconnect
                    LOGGER.error( "Fail to connect to server " + host + ":" + port );
					reconnect();
				}
                connectTriedTimes.incrementAndGet();
			}
			
		});
	}

	@Override
	public int read(byte[] buf, int off, int len) throws TTransportException {
		throw new TTransportException( "not support this operation");
	}

	@Override
	public void write(byte[] buf, int off, int len) throws TTransportException {
		throw new TTransportException( "not support this operation");
		
	}


	private class MessageHandler extends ChannelInboundHandlerAdapter {
	    @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
            listener.msgReceived( TNonblockingNettySocket.this, (ByteBuffer)msg);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            LOGGER.error( "connection to server " + ctx.channel().remoteAddress() + " is broken");
        }
    }

}
