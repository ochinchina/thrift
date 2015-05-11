package org.apache.thrift.transport;

import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.spi.SelectorProvider;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TSelector {
	private static final Logger LOGGER = LoggerFactory.getLogger(TSelector.class);
	private SelectThread selectThread;
	
	public static interface ChannelOperator {
		void doOperation();
	}
	
	private static class ChannelOperation {
		SelectableChannel channel;
		int op;
		ChannelOperator operator;
		int times;
		
		
		
		public ChannelOperation( SelectableChannel channel, int op, ChannelOperator operator, int times ) {
			this.channel = channel;
			this.op = op;
			this.operator = operator;
			this.times = times;
		}
		
	}
	
	private ConcurrentLinkedQueue< ChannelOperation > pendingOperations = new ConcurrentLinkedQueue<ChannelOperation>();
	private ConcurrentLinkedQueue< ChannelOperation > cancelOperations = new ConcurrentLinkedQueue<ChannelOperation>();
	
	public TSelector() {
		try {
			selectThread = new SelectThread();
			selectThread.start();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void register( SelectableChannel channel, int op, ChannelOperator operator, int times ) {
		if( Thread.currentThread().getId() != selectThread.getId() ) {
			pendingOperations.add( new ChannelOperation(channel, op, operator, times ) );
			selectThread.getSelector().wakeup();	
		} else {
			selectThread.registerChannel( new ChannelOperation(channel, op, operator, times ) );
		}
	}
	
	public void cancel( SelectableChannel channel, int op, ChannelOperator operator ) {
		if( Thread.currentThread().getId() != selectThread.getId() ) {
			cancelOperations.add( new ChannelOperation(channel, op, operator, 0 ) );
			selectThread.getSelector().wakeup();	
		} else {
			selectThread.cancelChannel( new ChannelOperation(channel, op, operator, 0 ) );
		}
	}
	
	public void cancel(SelectableChannel channel, int op ) {
		cancel( channel, op, null );
		
	}
		
	private static class ChannelManager {
		private HashMap< SelectableChannel, HashMap< Integer, List< ChannelOperation> > > channelOperations = new HashMap< SelectableChannel, HashMap< Integer, List< ChannelOperation> > >();
		
		public void removeChannel(  ChannelOperation channelOp ) {
			HashMap< Integer, List< ChannelOperation> > opChannels = channelOperations.get( channelOp.channel );
			
			if( opChannels != null ) {
				if( channelOp.operator == null ) {
					opChannels.remove( channelOp.op );
				} else {
					List< ChannelOperation > operators = opChannels.get( channelOp.op );
					if( operators != null ) {
						
						operators.remove( channelOp );
						if( operators.isEmpty() ) {
							opChannels.remove( channelOp.op );
						}
					}
				}
				
				if( opChannels.isEmpty() ) {
					channelOperations.remove( channelOp.channel );
				}
			}
			
		}
		
		public void addChannel(  ChannelOperation channelOp ) {
			HashMap< Integer, List< ChannelOperation> > opChannels = channelOperations.get( channelOp.channel );
			if( opChannels == null ) {
				opChannels = new HashMap< Integer, List< ChannelOperation> >();
				channelOperations.put( channelOp.channel, opChannels );
			}
			
			List< ChannelOperation > operators = opChannels.get( channelOp.op );
			if( operators == null ) {
				operators = new LinkedList<ChannelOperation>();
				opChannels.put( channelOp.op, operators );
			}
			
			operators.add( channelOp );
		}
		
		public int getOps( SelectableChannel channel ) {
			int ops = 0;
			HashMap< Integer, List< ChannelOperation> > opChannels = channelOperations.get( channel );
			if( opChannels != null ) {
				for( int i: opChannels.keySet() ) {
					ops |= i;
				}
			}
			return ops;			
		}
		
		public void doOperations( SelectableChannel channel, int readyOps ) {
			HashMap< Integer, List< ChannelOperation> > opChannels = channelOperations.get( channel );
			if( opChannels != null ) {
				
				for( int op: opChannels.keySet() ) {
					if( ( op & readyOps ) == op ) {
						List< ChannelOperation> operators = opChannels.get( op );
						
						if( operators != null && !operators.isEmpty() ) {
							try { 
								ChannelOperation channelOp = operators.get( 0 );
								if( channelOp.times > 0 ) {									
									channelOp.times --;
									if( channelOp.times <= 0 ) {
										operators.remove( 0 );
										if( operators.isEmpty() ) {
											opChannels.remove( op );
										}
									}
								}
								channelOp.operator.doOperation(); 
							} catch( Exception ex ) {
								
							}
						}
					}
				}				
			}
		}
		
		public void doOperationOnAll( SelectableChannel channel ) {
			HashMap< Integer, List< ChannelOperation> > opChannels = channelOperations.get( channel );
			
			if( opChannels != null ) {
				for( int op: opChannels.keySet() ) {
					List< ChannelOperation > operators = opChannels.get( op );
					if( operators != null ) {
						for( ChannelOperation channelOp: operators ) {
							try { channelOp.operator.doOperation(); } catch( Exception ex ) {}
						}
					}
				}
			}			
		}
		
		public void removeAllOperation( SelectableChannel channel ) {
			channelOperations.remove( channel );
		}
				
	}
	  private class SelectThread extends Thread {
		    private final Selector selector;
		    private volatile boolean running;
		    
		    private ChannelManager channelMgr = new ChannelManager();

		    public SelectThread() throws IOException {
		      this.selector = SelectorProvider.provider().openSelector();
		      this.running = true;
		      this.setName("TSelector#SelectorThread " + this.getId());

		      // We don't want to hold up the JVM when shutting down
		      setDaemon(true);
		    }
		    

		    public Selector getSelector() {
		      return selector;
		    }

		    public void finish() {
		      running = false;
		      selector.wakeup();
		    }

		    public void run() {
		      while (running) {
				try {
		          doSelect();
		          transitionChannels();
		          startPendingChannels();
		          cancelChannels();
		        } catch (Exception exception) {
		        	exception.printStackTrace();
		          LOGGER.error("Ignoring uncaught exception in SelectThread", exception);
		        }
		      }
		    }


			private void doSelect() {
				try {
					if( pendingOperations.isEmpty() && cancelOperations.isEmpty() ) {
						selector.select();
					} else {
						selector.selectNow();
					}
		          } catch (IOException e) {
		        	  e.printStackTrace();
		            LOGGER.error("Caught IOException in TAsyncClientManager!", e);
		          }
			}

		    private void transitionChannels() {
		    	Iterator<SelectionKey> keys = selector.selectedKeys().iterator();
		    	
		    	while (keys.hasNext()) {
		    		SelectionKey key = keys.next();
		    		processSelectedKey(key);		    		
		    	}
		    }
		    
		    private void cancelChannels() {
		    	ChannelOperation channelOp;
				while( (  channelOp = cancelOperations.poll() ) != null ) {
					cancelChannel( channelOp );
				}	
		    }


			private void processSelectedKey(SelectionKey key) {
				if( !key.isValid() || !key.channel().isOpen()) {
					key.cancel();
					channelMgr.doOperationOnAll(key.channel());
					channelMgr.removeAllOperation(key.channel());
					return;
				}
				
				channelMgr.doOperations(key.channel(), key.readyOps() );
				int ops = channelMgr.getOps( key.channel() );
				key.interestOps(ops);				
			}
		    


			private void startPendingChannels() {
				ChannelOperation channelOp;
				while( (  channelOp = pendingOperations.poll() ) != null ) {
					registerChannel( channelOp );
				}				
			}


			void registerChannel(ChannelOperation channelOp) {
				channelMgr.addChannel(channelOp );
				try {
					channelOp.channel.register(selector, channelMgr.getOps(channelOp.channel ));
				} catch (ClosedChannelException e) {
					e.printStackTrace();
				}				
			}
			
			void cancelChannel( ChannelOperation channelOp ) {
				channelMgr.removeChannel(channelOp );
				try {
					channelOp.channel.register(selector, channelMgr.getOps(channelOp.channel ));
				} catch (ClosedChannelException e) {
					e.printStackTrace();
				}	
			}
			
			
			
	  }
	
}

	
