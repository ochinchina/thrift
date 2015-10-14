#include "TAsyncSocketChannel.h"
#include "BackGroundIOService.h"
#include "TAsyncUtil.h"
#include <concurrency/PlatformThreadFactory.h>

namespace apache { namespace thrift { namespace async {


TAsyncSocketChannel::TAsyncSocketChannel( const std::string& serverAddr,
                const std::string& port,
                boost::shared_ptr< ::apache::thrift::protocol::TProtocolFactory > protocolFactory,
                int timeoutMillis,
				int replyProcThreadNum )
:TAsyncDispatchableChannel( protocolFactory, timeoutMillis ),
stop_( false ),
connected_( false ),
io_service_( BackGroundIOService::getInstance().get_io_service() ),
serverAddr_( serverAddr ),
serverPort_( port ),
sock_( new boost::asio::ip::tcp::socket( io_service_ ) ),
threadManager_( TAsyncUtil::createThreadManager( replyProcThreadNum ) )
{
}

TAsyncSocketChannel::TAsyncSocketChannel( boost::asio::io_service& io_service,
                const std::string& serverAddr,
                const std::string& port,
                boost::shared_ptr< ::apache::thrift::protocol::TProtocolFactory > protocolFactory,
                int timeoutMillis,
				int replyProcThreadNum )
:TAsyncDispatchableChannel( protocolFactory, timeoutMillis ),
stop_( false ),
connected_( false ),
io_service_( io_service ),
serverAddr_( serverAddr ),
serverPort_( port ),
sock_( new boost::asio::ip::tcp::socket( io_service_ ) ),
threadManager_( TAsyncUtil::createThreadManager( replyProcThreadNum ) )
{
}

TAsyncSocketChannel::TAsyncSocketChannel( boost::shared_ptr<boost::asio::ip::tcp::socket> sock,
                        boost::shared_ptr< ::apache::thrift::protocol::TProtocolFactory > protocolFactory,
                        int timeoutMillis,
                        int replyProcThreadNum )
:TAsyncDispatchableChannel( protocolFactory, timeoutMillis ),
stop_( false ),
connected_( sock_->is_open() ),
io_service_( sock->io_service() ),
sock_( sock ),
threadManager_( TAsyncUtil::createThreadManager( replyProcThreadNum ) )
{
}

void TAsyncSocketChannel::start( const boost::function< void() >& connCb ) {
    if( connected_ ) {
            startRead( boost::shared_array<char>( new char[4096]), 4096, connCb );

    } else {
            startConnect( connCb );
    }
}

void TAsyncSocketChannel::stop() {
    stop_ = true;
    TAsyncDispatchableChannel::stop();
}


bool TAsyncSocketChannel::isConnected() const {
    return connected_;
}

void TAsyncSocketChannel::sendMessage( const std::string& msg, const boost::function< void( bool ) >& callback ) {

        if( stop_ ) {
                callback( false );
                return;
        }
        boost::shared_ptr<std::string> s( new std::string() );

        //add length header
        size_t n = msg.length();
        char buf[4];

        buf[0] = ( n >> 24 ) & 0xff;
        buf[1] = ( n >> 16 ) & 0xff;
        buf[2] = ( n >> 8 ) & 0xff;
        buf[3] = n & 0xff;

        s->append( buf, 4 );
        s->append( msg );

        //write the message
        boost::asio::async_write( *sock_,
                                        boost::asio::buffer( s->data(), s->length() ),
                                        boost::asio::transfer_all(),
                                        boost::bind( &sendFinished, _1, _2, s, callback ) );
}
/**
 *  @Override TAsyncDispatchableChannel#startTimer
 */
void TAsyncSocketChannel::startTimer( int timeoutInMillis, const boost::function<void()>& callback ) {
        boost::shared_ptr<boost::asio::deadline_timer> timer( new boost::asio::deadline_timer(io_service_) );

        timer->expires_from_now( boost::posix_time::milliseconds( timeoutInMillis ));
        timer->async_wait( boost::bind( &TAsyncSocketChannel::handleTimeout, _1, callback, timer ) );
}


void TAsyncSocketChannel::handleTimeout( const boost::system::error_code& error, boost::function<void()> callback, boost::shared_ptr<boost::asio::deadline_timer> timer ) {
        callback();
}


void TAsyncSocketChannel::startRead( boost::shared_array<char> buf, size_t size, const  boost::function<void()>& connCb ) {
        sock_->async_read_some( boost::asio::buffer( buf.get(), size),
                                boost::bind( &TAsyncSocketChannel::dataReceived, this, _1, _2, buf, size, connCb ) );
}

void TAsyncSocketChannel::dataReceived( const boost::system::error_code& error,
                        std::size_t bytes_transferred,
                        boost::shared_array<char> buf,
                        size_t buf_size,
			boost::function<void()> connCb ) {
        if( error ) {
                connected_ = false;
                recvPackets_.clear();
                startConnect( connCb );
        }else{
                recvPackets_.append(  buf.get(), bytes_transferred );
                startRead( buf, buf_size, connCb );
                processPackets();
        }
}

void TAsyncSocketChannel::startConnect( const boost::function< void() >& connCb ) {
        //if no server address and port provided, don't do the connect
        if( !serverAddr_.empty() && !serverPort_.empty() ) {
                //query the address
                boost::asio::ip::tcp::resolver resolver( io_service_ );
                boost::asio::ip::tcp::resolver::query query( serverAddr_, serverPort_ );
                boost::system::error_code err;
                boost::asio::ip::tcp::resolver::iterator iter = resolver.resolve( query, err );

                //if not resolve the address, start a reconnect timer and connect later
                if( err || iter == boost::asio::ip::tcp::resolver::iterator() ) {
                        startReconnectTimer( connCb );
                } else {
                        sock_->close( err );
                        //it is good to solve the address, try to connect to it
                        boost::asio::ip::tcp::endpoint endpoint = *iter;
                        sock_->async_connect( endpoint, boost::bind( &TAsyncSocketChannel::handleConnect, this, _1, connCb ) );
                }
        }
}


void TAsyncSocketChannel::handleConnect(const boost::system::error_code& error, boost::function< void() > connCb ) {
        if( error ) {
                startReconnectTimer( connCb );
        } else {//if connected, start to read the data from server
                connected_ = true;
		if( !connCb.empty() ) {
			connCb();
		}
                startRead( boost::shared_array<char>( new char[4096] ), 4096, connCb );
        }
}
void TAsyncSocketChannel::startReconnectTimer( const boost::function< void() >& connCb ) {
        boost::shared_ptr<boost::asio::deadline_timer> timer( new boost::asio::deadline_timer(io_service_ ) );

        timer->expires_from_now( boost::posix_time::seconds(5));
        timer->async_wait( boost::bind( &TAsyncSocketChannel::handleReconnectTimeout, this, _1, timer, connCb ) );             
}

void TAsyncSocketChannel::handleReconnectTimeout( const boost::system::error_code& error, boost::shared_ptr<boost::asio::deadline_timer> timer, boost::function< void() > connCb ) {
        startConnect( connCb );
}

void TAsyncSocketChannel::processPackets() {
        while( recvPackets_.length() > 4 ) {
                //at first read the length
                int32_t n = TAsyncUtil::readInt( recvPackets_ );
                if( recvPackets_.length() < ( 4 + n ) ) {
                        break;
                } else {
                        std::string msg = recvPackets_.substr( 4, n );
                        recvPackets_.erase( 0, 4 + n );
						threadManager_->add( TAsyncUtil::createTask( boost::bind( &TAsyncSocketChannel::processPacket, this, msg ) ) );
                }
        }
}

void TAsyncSocketChannel::processPacket( std::string msg ) {
	recvMessage( msg );
}


void TAsyncSocketChannel::sendFinished( const boost::system::error_code& error,
                                std::size_t bytes_transferred,
                                boost::shared_ptr< std::string > data,
                                boost::function< void( bool ) > callback ) {
        callback( !error );
}
}}}//end namespace
