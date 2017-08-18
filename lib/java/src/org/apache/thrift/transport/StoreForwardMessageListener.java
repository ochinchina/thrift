package org.apache.thrift.transport;

import java.nio.ByteBuffer;
import java.util.LinkedList;

/**
 * Created by stou on 2017/8/11.
 */
public class StoreForwardMessageListener implements TNonblockingMessageListener {

    private TNonblockingTransport transport;
    private  TNonblockingMessageListener listener;
    private LinkedList<ByteBuffer> msgs = new LinkedList<>();

    public StoreForwardMessageListener( TNonblockingTransport transport) {
        this.transport = transport;
    }

    public synchronized void setListener( TNonblockingMessageListener listener) {
        this.listener = listener;
        while( !msgs.isEmpty()) {
            this.listener.msgReceived( this.transport, msgs.removeFirst() );
        }
    }
    @Override
    public synchronized void msgReceived(TNonblockingTransport transport, ByteBuffer msgBuf) {
        if( listener == null ) {
            this.msgs.add( msgBuf );
        } else {
            this.listener.msgReceived( transport, msgBuf);
        }
    }

    @Override
    public synchronized void exception(TNonblockingTransport transport, Exception ex) {
        if( this.listener != null ) {
            this.listener.exception( transport, ex );
        }
    }
}