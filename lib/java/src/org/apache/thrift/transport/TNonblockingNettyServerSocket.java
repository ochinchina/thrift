package org.apache.thrift.transport;


import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TNonblockingNettyServerSocket extends TNonblockingServerTransport {
    private static final Logger LOGGER = LoggerFactory.getLogger(TNonblockingNettyServerSocket.class);

	private EventLoopGroup bossGroup = new NioEventLoopGroup();
	private EventLoopGroup workerGroup = new NioEventLoopGroup();
	private ServerBootstrap bootstrap = new ServerBootstrap();
	private InetSocketAddress bindAddr;
	private TransportAcceptCallback acceptCallback;
    private ConcurrentHashMap<Channel, ClientTransport> clients = new ConcurrentHashMap<>();
	/**
	 * Creates just a port listening server socket
	 */
	public TNonblockingNettyServerSocket(int port) throws TTransportException {
		this(port, 0);
	}

    public TNonblockingNettyServerSocket( String bindAddr, int port) throws TTransportException {
        this(bindAddr, port, 0);
    }

	/**
	 * Creates just a port listening server socket
	 */
	public TNonblockingNettyServerSocket(int port, int clientTimeout) throws TTransportException {
		this(new InetSocketAddress(port), clientTimeout);
	}

    public TNonblockingNettyServerSocket(String bindAddr, int port, int clientTimeout) throws TTransportException {
        this(new InetSocketAddress(bindAddr, port), clientTimeout);
    }

    public TNonblockingNettyServerSocket(InetSocketAddress bindAddr) throws TTransportException {
		this(bindAddr, 0);
	}

	public TNonblockingNettyServerSocket(InetSocketAddress bindAddr, int clientTimeout) throws TTransportException {
		this.bindAddr = bindAddr;
		bootstrap.group(bossGroup, workerGroup)
				.channel(NioServerSocketChannel.class)
				.childHandler( new ChannelInitializer<SocketChannel>() {

					@Override
					protected void initChannel(SocketChannel ch) throws Exception {
						ch.pipeline().addLast( new TNettyMessageDecoder(), new MessageHandler() );
					}
				}).option(ChannelOption.SO_BACKLOG, 128)
				.childOption( ChannelOption.TCP_NODELAY, true )
				.childOption( ChannelOption.SO_KEEPALIVE, true );
	}

	@Override
	public void accept( final TransportAcceptCallback acceptCallback) {
		this.acceptCallback = acceptCallback;

		bootstrap.bind( bindAddr ).addListener(new ChannelFutureListener() {
			@Override
			public void operationComplete(ChannelFuture future) throws Exception {
				if( future.isSuccess() ) {
				    LOGGER.info( "Successful listen on " + future.channel().localAddress());
					//bind successfully, monitor the close
					future.channel().closeFuture().addListener(new ChannelFutureListener() {
						@Override
						public void operationComplete(ChannelFuture cf ) throws Exception {
							//the acceptor is closed
                            LOGGER.info( "the listener on " + cf.channel().localAddress() + " is closed");
						}
					});
				} else {
                    LOGGER.error( "Fail to listen on " + future.channel().localAddress());
                }
			}
		});
		
	}

	@Override
	public void listen() throws TTransportException {
		//NOTHING TO DO, the listen function is in the accept() method
		
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}

	@Override
	protected TTransport acceptImpl() throws TTransportException {
		// TODO Auto-generated method stub
		return null;
	}


	private class ClientTransport extends TNonblockingTransport {
	        private Channel channel;
            private StoreForwardMessageListener listener = new StoreForwardMessageListener( this );

            public ClientTransport( Channel channel ) {
                this.channel = channel;
            }
            @Override
            public boolean isOpen() {
                return channel.isOpen();
            }

            @Override
            public void open() throws TTransportException {

            }

            @Override
            public void close() {
                channel.close();
            }

            @Override
            public int read(byte[] buf, int off, int len) throws TTransportException {
                return 0;
            }

            @Override
            public void write(byte[] buf, int off, int len) throws TTransportException {

            }

            @Override
            public void setMessageListener(TNonblockingMessageListener listener) {
                this.listener.setListener( listener );
            }

            @Override
            public void start() throws IOException {

            }

            @Override
            public void asyncWrite(ByteBuffer buffer, final AsyncWriteCallback cb) throws IOException {
                ByteBuf data = channel.alloc().buffer(buffer.remaining()).writeBytes(buffer);
                channel.writeAndFlush( data ).addListener(new ChannelFutureListener() {
                    @Override
                    public void operationComplete(ChannelFuture future) throws Exception {
                        if( future.isSuccess()) {
                            cb.writeFinished( true );
                        } else {
                            cb.writeFinished( false );
                            LOGGER.error( "Fail to send message to client " + future.channel().remoteAddress());
                        }
                    }
                });
            }

        public void connectionLost() {
            this.listener.exception( this, new RuntimeException("Connection lost"));
        }

        public void msgReceived(ByteBuffer msg) {
            this.listener.msgReceived( this, msg );
        }
    }



	private class MessageHandler extends ChannelInboundHandlerAdapter {
	    @Override
        public void channelActive(ChannelHandlerContext ctx)  throws Exception {
            ClientTransport clientTransport = new ClientTransport(ctx.channel());
            clients.put( ctx.channel(), clientTransport);
            LOGGER.info( "Accept a connection from " + ctx.channel().remoteAddress());
            acceptCallback.accepted( clientTransport );
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx)  throws Exception {
            LOGGER.info( "Connection with " + ctx.channel().remoteAddress() + " is lost");
            ClientTransport clientTransport = clients.remove(ctx.channel());
            if( clientTransport != null ) {
                clientTransport.connectionLost();
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            LOGGER.info( "Connection with " + ctx.channel().remoteAddress() + " is lost");
            ClientTransport clientTransport = clients.remove(ctx.channel());
            if( clientTransport != null ) {
                clientTransport.connectionLost();
            }
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
            clients.get( ctx.channel() ).msgReceived( (ByteBuffer)msg );
        }
	};

}
