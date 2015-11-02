import java.net.Socket;
import java.util.LinkedList;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.thrift.transport.TNonblockingMemoryTransport;



public class SockChannelMemoryTransportManager {
	private ConcurrentHashMap< SocketChannel, TNonblockingMemoryTransport > sockChannelTransports = new ConcurrentHashMap<>();
	
	public TNonblockingMemoryTransport createTransport(Socket sock,
			int channel) {
		TNonblockingMemoryTransport trans = new TNonblockingMemoryTransport();
		sockChannelTransports.put( new SocketChannel( sock, channel ), trans );
		return trans;
	}
	

	public TNonblockingMemoryTransport getTransport(Socket sock, int channel) {
		return sockChannelTransports.get( new SocketChannel( sock, channel ));
	}
	
	public TNonblockingMemoryTransport removeTransport( Socket sock, int channel ) {
		return sockChannelTransports.remove( new SocketChannel( sock, channel ));
	}
	
	public void removeTransports( Socket socket ) {
		LinkedList< SocketChannel > removedSockChannels = new LinkedList<>();
		
		for( SocketChannel sockChannel: sockChannelTransports.keySet() ) {
			if( sockChannel.sock.equals( socket )) {
				removedSockChannels.add( sockChannel );
			}
		}
		
		for( SocketChannel sockChannel: removedSockChannels ) {
			sockChannelTransports.remove( sockChannel );
		}
	}
	
	private class SocketChannel {
		Socket sock;
		int channel;
		
		public SocketChannel( Socket sock, int channel ) {
			this.sock = sock;
			this.channel = channel;
		}
		
		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + channel;
			result = prime * result + ((sock == null) ? 0 : sock.hashCode());
			return result;
		}
		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			SocketChannel other = (SocketChannel) obj;
			if (channel != other.channel)
				return false;
			if (sock == null) {
				if (other.sock != null)
					return false;
			} else if (!sock.equals(other.sock))
				return false;
			return true;
		}				
	}
}
