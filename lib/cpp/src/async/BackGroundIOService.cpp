#include "BackGroundIOService.h"
#include <boost/thread.hpp>
#include <boost/bind.hpp>

namespace apache { namespace thrift { namespace async {

BackGroundIOService& BackGroundIOService::getInstance() {
	static BackGroundIOService* instance = 0;
	static boost::mutex m;

	if( instance == 0 ) {
		boost::lock_guard< boost::mutex > lock( m );
		if( instance == 0 ) {
			instance = new BackGroundIOService();
		}
	}

	return *instance;
}

boost::asio::io_service& BackGroundIOService::get_io_service() {
	return io_service_;
}

void BackGroundIOService::stop() {
	if( stop_ ) {
            return;
        }
        stop_ = true;
        io_service_.stop();
        serviceRunThread_.join();
}
    
BackGroundIOService::BackGroundIOService()
:stop_( false ),
io_service_work_( io_service_ ),
serviceRunThread_( boost::bind( &BackGroundIOService::run, this ) )
{
}

BackGroundIOService::~BackGroundIOService() {
	stop();
}

void BackGroundIOService::run() {
	while( !stop_ ) {
		try {
			io_service_.run();
		}catch( ... ) {
		}
	}
}

}}}//end of namesapce
