#ifndef _BACK_GROUND_IO_SERVICE_H
#define _BACK_GROUND_IO_SERVICE_H

#include <boost/asio.hpp>
#include <boost/thread.hpp>

namespace apache { namespace thrift { namespace async {
class BackGroundIOService {
public:

    static BackGroundIOService& getInstance();

    boost::asio::io_service& get_io_service();

    void stop();
private:
    BackGroundIOService();

    ~BackGroundIOService();
    void run();
private:
    volatile bool stop_;
    boost::asio::io_service io_service_;
    boost::asio::io_service::work io_service_work_;
    boost::thread serviceRunThread_;
};

}}}

#endif/*_BACK_GROUND_IO_SERVICE_H*/

