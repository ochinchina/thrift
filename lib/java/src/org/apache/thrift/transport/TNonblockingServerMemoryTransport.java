package org.apache.thrift.transport;

import java.util.LinkedList;


public class TNonblockingServerMemoryTransport extends TNonblockingServerTransport{

	private TransportAcceptCallback acceptCallback = null;
	private LinkedList<TNonblockingTransport> acceptedTrans = new LinkedList<>();
	
	@Override
	public void accept(TransportAcceptCallback acceptCallback) {
		synchronized( acceptedTrans ) {
			this.acceptCallback = acceptCallback;
			while( !acceptedTrans.isEmpty()) {
				this.acceptCallback.accepted(acceptedTrans.removeFirst());
			}
		}
	}

	@Override
	public void listen() throws TTransportException {
	}

	@Override
	public void close() {
		
	}

	@Override
	protected TTransport acceptImpl() throws TTransportException {
		throw new TTransportException( "Not support");
	}
	
	public void connectionAccepted(TNonblockingTransport trans) {
		synchronized( acceptedTrans ) {
			if( acceptCallback == null ) {
				acceptedTrans.add(trans);
			} else {
				acceptCallback.accepted(trans);
			}
		}		
	}

}
