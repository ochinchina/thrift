#ifndef _TASYNC_DISPATCHABLE_CHANNEL_H
#define _TASYNC_DISPATCHABLE_CHANNEL_H

#include <async/TAsyncChannel.h>
#include <protocol/TProtocol.h>
#include <transport/TBufferTransports.h>
#include <string>
#include <boost/function.hpp>
#include <boost/bind.hpp>
#include <boost/smart_ptr.hpp>
#include <boost/thread.hpp>

namespace apache { namespace thrift { namespace async {
class TAsyncDispatchableChannel: public apache::thrift::async::TAsyncChannel {
public:
    /**
     *
     * @Override  TAsyncChannel#sendAndRecvMessage()
     */
    virtual void sendAndRecvMessage(const VoidCallback& cob,
	        apache::thrift::transport::TMemoryBuffer* sendBuf,
	        apache::thrift::transport::TMemoryBuffer* recvBuf);
	virtual void sendMessage(const VoidCallback& cob, apache::thrift::transport::TMemoryBuffer* message);
	virtual void recvMessage(const VoidCallback& cob, apache::thrift::transport::TMemoryBuffer* message);
	virtual bool good() const;
	virtual bool error() const;
	virtual bool timedOut() const;
protected:
        /**
         *  create a channel
         *
         * @param protocolFactory the protocol factory
         * @param timeoutMillis > 0 the request timeout in millis, <=0 no timeout for request
         * @param replyProcThreadNum number the threads to process the reply from server side
         */
        TAsyncDispatchableChannel( boost::shared_ptr< ::apache::thrift::protocol::TProtocolFactory > protocolFactory,
                                                int timeoutMillis );
                                                
        /**
         *  send message to peer
         *
         * @param msg - the message sent to peer
         * @param callback - will be called after the message send is finished
         */
        virtual void sendMessage( const std::string& msg, const boost::function< void( bool ) >& callback ) = 0;

        /**
         *  if a message is received from peer, this method should be called
         *
         * @param msg - the message received from peer
         */
        void recvMessage( const std::string& msg ) ;
        /**
         *start a timer with timeout
         */
        virtual void startTimer( int timeoutInMillis, const boost::function<void()>& callback ) = 0;

        void stop();
private:
        TAsyncDispatchableChannel();
        void sendFinished( bool success, int32_t seqId );
        void handleRequestTimeout( int32_t seqId ) ;
        int32_t extractSeqId( const std::string& msg );
        int32_t extractSeqId( uint8_t* buf, uint32_t n ) ;
        static boost::shared_ptr<apache::thrift::transport::TMemoryBuffer> clone( apache::thrift::transport::TMemoryBuffer& buf );
        static void copy( apache::thrift::transport::TMemoryBuffer& src, apache::thrift::transport::TMemoryBuffer& dest );
private:
        struct RespInfo;
        class RespInfoManager;
        class SeqIdAllocator;
        boost::mutex mutex_;
        int timeoutMillis_;
        boost::shared_ptr<SeqIdAllocator> seqIdAllocator_;
        boost::shared_ptr<RespInfoManager> respInfoMgr_;
        boost::shared_ptr< ::apache::thrift::protocol::TProtocolFactory > protocolFactory_;
};

}}}//end namespace

#endif/*_TASYNC_DISPATCHABLE_CHANNEL_H*/
