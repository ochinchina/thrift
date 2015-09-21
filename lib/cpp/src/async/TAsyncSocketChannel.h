#ifndef _TASYNC_SOCKET_CHANNEL_H
#define _TASYNC_SOCKET_CHANNEL_H

#include "TAsyncDispatchableChannel.h"
#include <boost/asio.hpp>

namespace apache { namespace thrift { namespace async {

/**
 * This class repsents the thrift async socket transport
 *
 */
class TAsyncSocketChannel: public TAsyncDispatchableChannel {
public:
        /**
         *  create a socket channel
         *
         * @param serverAddr the server address
         * @param port the port number or service name
         * @param protocolFactory the thrift protocol factory
         * @param timeoutMillis > 0 the request timeout in milliseconds, <=0 no timeout for request
         * @param replyProcThreadNum the number of threads will be created to process the reply from
         *       server side, this parameter must be greater than 0
         */
        TAsyncSocketChannel( const std::string& serverAddr,
                        const std::string& port,
                        boost::shared_ptr< ::apache::thrift::protocol::TProtocolFactory > protocolFactory,
                        int timeoutMillis,
                        int replyProcThreadNum );
        /**
         *  create a socket channel
         *
         * @param io_service the boost io_service used by this channel
         * @param serverAddr the server address
         * @param port the port number or service name
         * @param protocolFactory the thrift protocol factory
         * @param timeoutMillis > 0 the request timeout in milliseconds, <=0 no timeout for request
         * @param replyProcThreadNum the number of threads will be created to process the reply from
         *       server side, this parameter must be greater than 0
         */
        TAsyncSocketChannel( boost::asio::io_service& io_service,
                        const std::string& serverAddr,
                        const std::string& port,
                        boost::shared_ptr< ::apache::thrift::protocol::TProtocolFactory > protocolFactory,
                        int timeoutMillis,
                        int replyProcThreadNum );
        /**
         * create a socket channel
         *
         * @param sock the already connected socket to the server side
         * @param protocolFactory the thrift protocol factory
         * @param timeoutMillis > 0 the request timeout in milliseconds, <=0 no timeout for request
         * @param replyProcThreadNum the number of threads will be created to process the reply from
         *       server side, this parameter must be greater than 0
         */
        TAsyncSocketChannel( boost::shared_ptr<boost::asio::ip::tcp::socket> sock,
                                boost::shared_ptr< ::apache::thrift::protocol::TProtocolFactory > protocolFactory,
                                int timeoutMillis,
                                int replyProcThreadNum );
        /**
         *  startup the channel for using
         */
        void start();

        /**
         *  stop the channel
         */
        void stop();

        /**
         * @return true if this socket channel is already connected to server
         */
        bool isConnected() const;
protected:
        /**
         *  @Override TAsyncDispatchableChannel#sendMessage
         */
        virtual void sendMessage( const std::string& msg, const boost::function< void( bool ) >& callback );
        /**
         *  @Override TAsyncDispatchableChannel#startTimer
         */
        virtual void startTimer( int timeoutInMillis, const boost::function<void()>& callback );
private:

        static void handleTimeout( const boost::system::error_code& error, boost::function<void()> callback, boost::shared_ptr<boost::asio::deadline_timer> timer );


        void startRead( boost::shared_array<char> buf, size_t size );

        void dataReceived( const boost::system::error_code& error,
                                std::size_t bytes_transferred,
                                boost::shared_array<char> buf,
                                size_t buf_size );
        /**
         *  start the connection to the peer
         */
        void startConnect() ;

        /**
         * the callback for async_connect()
         */
        void handleConnect(const boost::system::error_code& error);
        void startReconnectTimer();

        void handleReconnectTimeout( const boost::system::error_code& error, boost::shared_ptr<boost::asio::deadline_timer> timer );

        void processPackets();

        int32_t readInt( const std::string& s );

        static void sendFinished( const boost::system::error_code& error,
                                        std::size_t bytes_transferred,
                                        boost::shared_ptr< std::string > data,
                                        boost::function< void( bool ) > callback );
private:
        volatile bool stop_;
        volatile bool connected_;
        boost::asio::io_service& io_service_;
        std::string serverAddr_;
        std::string serverPort_;
        std::string recvPackets_;
        boost::shared_ptr<boost::asio::ip::tcp::socket> sock_;
        ::apache::thrift::protocol::TProtocolFactory* protocolFactory_;
};

}}}//end of namespace

#endif/*_TASYNC_SOCKET_CHANNEL_H*/
