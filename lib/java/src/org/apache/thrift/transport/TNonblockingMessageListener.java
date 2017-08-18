package org.apache.thrift.transport;

import java.nio.ByteBuffer;

public interface TNonblockingMessageListener {
	void msgReceived( TNonblockingTransport transport, ByteBuffer msgBuf);
	void exception( TNonblockingTransport transport, Exception ex );
}
