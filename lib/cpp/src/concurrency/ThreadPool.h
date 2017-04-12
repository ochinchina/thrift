/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#ifndef _THRIFT_CONCURRENCY_THREADPOOL_H_
#define _THRIFT_CONCURRENCY_THREADPOOL_H_ 1

#include <boost/shared_ptr.hpp>
#include <boost/function.hpp>

namespace apache { namespace thrift { namespace concurrency {



/**
 * ThreadPool class
 *
 */
class ThreadPool {

public:
    ThreadPool( int threads )
    :stop_( false )
    {
        for( int i = 0; i < threads; i++ ) {
            threads_.push_back( boost::shared_ptr<boost::thread>( new boost::thread( boost::bind( &ThreadPool::takeAndExecTask, this ) ) ) );
        }
    }
    
    ~ThreadPool() {
        shutdown();
    }
    
    void shutdown() {
        if( !stop_ ) {
            stop_ = true;
            boost::unique_lock<boost::mutex> lock(mutex_);
            while( !threads_.empty() ) {
                boost::shared_ptr<boost::thread> th = threads_.front();
                threads_.pop_front();
                if( th->joinable() ) {
                    th->interrupt();
                    th->join();
                }
            } 
        }
    }
    
    void submit( const boost::function<void()>& task ) {
    
        {
            boost::unique_lock<boost::mutex> lock(mutex_);
            
            tasks_.push_back( task );
        }
        cond_.notify_one();
    }
    
private:
    void takeAndExecTask() {
        while( !stop_ ) {
            boost::function<void()> task = takeTask();
            
            if( !task.empty() ) {
                try {
                    task();
                }catch( ... ) {
                }
            }
        }
    }
    
    boost::function<void()> takeTask() {    
        boost::unique_lock<boost::mutex> lock(mutex_);
        while( !stop_ ) {
            if( tasks_.empty() ) {
                cond_.wait(lock);
            } else {
                boost::function<void()> task = tasks_.front();
                tasks_.pop_front();
                return task;
            }
        }
    }    
private:
    volatile bool stop_;
    boost::mutex mutex_;
    boost::condition_variable cond_;
    std::list< boost::function<void()> > tasks_;
    std::list< boost::shared_ptr<boost::thread> > threads_;

};

}}} // apache::thrift::concurrency

#endif // #ifndef _THRIFT_CONCURRENCY_THREADMPOOL_H_
