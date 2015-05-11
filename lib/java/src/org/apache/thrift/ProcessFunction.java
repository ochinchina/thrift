/**
 * 
 */
package org.apache.thrift;

import org.apache.thrift.async.AsyncMethodCallback;
import org.apache.thrift.protocol.TMessage;
import org.apache.thrift.protocol.TMessageType;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolException;

public abstract class ProcessFunction<I, T extends TBase, R extends TBase> {
  private final String methodName;
  private boolean async = false;

  public ProcessFunction(String methodName) {
    this.methodName = methodName;
  }
  
  public ProcessFunction(String methodName, boolean async ) {
	    this.methodName = methodName;
	    this.async = async;
	  }
  

  public final void process(int seqid, TProtocol iprot, TProtocol oprot, I iface) throws TException {
    if( async ) {
    	asyncProcess( seqid, iprot, oprot, iface );
    } else {
    	syncProcess( seqid, iprot, oprot, iface );
    }
  }
  
  private void syncProcess( int seqid, TProtocol iprot, TProtocol oprot, I iface ) throws TException {
	  T args = getEmptyArgsInstance();
	    try {
	      args.read(iprot);
	    } catch (TProtocolException e) {
	      iprot.readMessageEnd();
	      sendException( seqid, oprot, e );
	      return;
	    }
	    iprot.readMessageEnd();
	    TBase result = getResult(iface, args);
	    sendResult( result, seqid, oprot );
  }
  
  private void asyncProcess( final int seqid, TProtocol iprot, final TProtocol oprot, I iface ) throws TException {
	  T args = getEmptyArgsInstance();
	    try {
	      args.read(iprot);
	    } catch (TProtocolException e) {
	      iprot.readMessageEnd();
	      sendException( seqid, oprot, e );
	      return;
	    }
	    iprot.readMessageEnd();
	    
	    getResult( iface, args, new AsyncMethodCallback<R>() {

			@Override
			public void onComplete(R result) {
				try {
					sendResult( result, seqid, oprot );
				} catch (TException e) {
				}
			}

			@Override
			public void onError(Exception e) {
				sendException( seqid, oprot, e);
			}
	    	
	    });
  }
  
  private void sendResult( TBase result,  int seqid, TProtocol oprot ) throws TException {
	  	oprot.writeMessageBegin(new TMessage(getMethodName(), TMessageType.REPLY, seqid));
		result.write(oprot);
		oprot.writeMessageEnd();
		oprot.getTransport().flush();
  }
  
  private void sendException( int seqid, TProtocol oprot, Exception e ) {
	  TApplicationException x = new TApplicationException(TApplicationException.PROTOCOL_ERROR, e.getMessage());
      try {
		oprot.writeMessageBegin(new TMessage(getMethodName(), TMessageType.EXCEPTION, seqid));
	     x.write(oprot);
	     oprot.writeMessageEnd();
	     oprot.getTransport().flush();
	} catch (TException e1) {
	}
  }

  protected abstract TBase getResult(I iface, T args) throws TException;
  
  protected abstract void getResult(I iface, T args, AsyncMethodCallback<R> resultCb ) throws TException;

  protected abstract T getEmptyArgsInstance();

  public String getMethodName() {
    return methodName;
  }
}