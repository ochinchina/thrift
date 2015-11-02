package org.apache.thrift.transport;

import java.io.IOException;
import java.nio.ByteBuffer;



public class TNonblockingMemoryTransport extends TNonblockingTransport {
	
	public static interface MessageWriter {

		void write(ByteBuffer msg, AsyncWriteCallback cb);

	}

	private TNonblockingMessageListener listener;
	private MessageWriter msgWriter = null;

	
	@Override
	public void setMessageListener(TNonblockingMessageListener listener) {
		this.listener = listener;		
	}

	@Override
	public void start() throws IOException {
	}

	@Override
	public void asyncWrite(ByteBuffer buffer, AsyncWriteCallback cb) throws IOException {
		MessageWriter writer = this.msgWriter;
		if( writer != null ) {
			writer.write(buffer, cb);
		}
	}

	@Override
	public boolean isOpen() {
		return true;
	}

	@Override
	public void open() throws TTransportException {
		
	}

	@Override
	public void close() {
	}

	@Override
	public int read(byte[] buf, int off, int len) throws TTransportException {
		throw new TTransportException( "Not read support");
	}

	@Override
	public void write(byte[] buf, int off, int len) throws TTransportException {
		throw new TTransportException( "Not write support");
		
	}

	public void messageReceived(ByteBuffer msg ) {
		TNonblockingMessageListener listener = this.listener;
		if( listener != null ) {
			listener.msgReceived(this, msg);
		}
		
	}

	public void setMessageWriter(MessageWriter messageWriter) {
		this.msgWriter = messageWriter;
		
	}

}
