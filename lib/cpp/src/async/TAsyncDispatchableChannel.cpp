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

#include "TAsyncDispatchableChannel.h"

namespace apache {namespace thrift { namespace async {
//
// defined to save the callback & the receive buffer for a request
//
struct TAsyncDispatchableChannel::RespInfo {
        VoidCallback cob;
        apache::thrift::transport::TMemoryBuffer* recvBuf;

        RespInfo( const VoidCallback& _cob,
                apache::thrift::transport::TMemoryBuffer* _recvBuf )
        :cob(_cob),
        recvBuf( _recvBuf )
        {
        }
};
//
// manage the RespInfo for a request
//
class TAsyncDispatchableChannel::RespInfoManager {
public:
        void addRespInfo( int32_t seqId, const VoidCallback& cob, apache::thrift::transport::TMemoryBuffer* recvBuf ) {
                boost::lock_guard<boost::mutex> lock( mutex_ );
                respInfos_[ seqId ].reset( new RespInfo( cob, recvBuf ) );
        }

        boost::shared_ptr<RespInfo> removeRespInfo( int32_t seqId ) {
                boost::lock_guard<boost::mutex> lock( mutex_ );

                std::map< int32_t, boost::shared_ptr<RespInfo> >::iterator iter = respInfos_.find( seqId );

                if( iter != respInfos_.end() ) {
                        boost::shared_ptr<RespInfo> info = iter->second;
                        respInfos_.erase( iter );
                        return info;
                }

                return boost::shared_ptr<RespInfo>();
        }
private:
        boost::mutex mutex_;
        std::map< int32_t, boost::shared_ptr<RespInfo> > respInfos_;
};


class TAsyncDispatchableChannel::SeqIdAllocator {
public:
	SeqIdAllocator()
	:nextSeqId_( 0 )
	{
	}
	
	int32_t allocSeqId() {
		boost::lock_guard<boost::mutex> lock( mutex_ );
		
		return nextSeqId_++;
	} 
private:
	int32_t nextSeqId_;
	boost::mutex mutex_;
};

void TAsyncDispatchableChannel::sendAndRecvMessage(const VoidCallback& cob,
    apache::thrift::transport::TMemoryBuffer* sendBuf,
    apache::thrift::transport::TMemoryBuffer* recvBuf) {

    boost::shared_ptr<apache::thrift::transport::TMemoryBuffer> tmpSendBuf = clone( *sendBuf );
    boost::shared_ptr< ::apache::thrift::protocol::TProtocol> prot = protocolFactory_->getProtocol( tmpSendBuf );
    int32_t rseqid = 0;
    std::string fname;
    ::apache::thrift::protocol::TMessageType mtype;

    try {
        //read the message header for reseting the seqId
        prot->readMessageBegin( fname, mtype, rseqid );
        boost::shared_ptr<apache::thrift::transport::TMemoryBuffer> localSendBuffer( new apache::thrift::transport::TMemoryBuffer() );

        prot = protocolFactory_->getProtocol( localSendBuffer );
        //allocate a sequence id
        int32_t seqId = seqIdAllocator_->allocSeqId();

        prot->writeMessageBegin( fname, mtype, seqId );

        copy( *tmpSendBuf, *localSendBuffer );

        respInfoMgr_->addRespInfo( seqId, cob, recvBuf );

        //
        //start the timeout timer if timeout is set
        //
        if( timeoutMillis_ > 0 ) {
            startTimer( timeoutMillis_, boost::bind( &TAsyncDispatchableChannel::handleRequestTimeout, this, seqId ) );
        }

        sendMessage( localSendBuffer->getBufferAsString(), boost::bind( &TAsyncDispatchableChannel::sendFinished, this, _1, seqId ) );
	}catch( ... ) {
    }
}

void TAsyncDispatchableChannel::sendMessage(const VoidCallback& cob, apache::thrift::transport::TMemoryBuffer* message) {
}

void TAsyncDispatchableChannel::recvMessage(const VoidCallback& cob, apache::thrift::transport::TMemoryBuffer* message) {
}

bool TAsyncDispatchableChannel::good() const { 
	return true;
}

bool TAsyncDispatchableChannel::error() const {
	return false;
}

bool TAsyncDispatchableChannel::timedOut() const {
	return false;
}

	
TAsyncDispatchableChannel::TAsyncDispatchableChannel( boost::shared_ptr< ::apache::thrift::protocol::TProtocolFactory > protocolFactory,
                                                int timeoutMillis )
:timeoutMillis_( timeoutMillis ),
seqIdAllocator_( new SeqIdAllocator() ),
respInfoMgr_( new RespInfoManager() ),
protocolFactory_( protocolFactory )
{
}

void TAsyncDispatchableChannel::recvMessage( const std::string& msg ) {
    try {
        int32_t seqId = extractSeqId( msg );
        boost::shared_ptr<RespInfo> respInfo = respInfoMgr_->removeRespInfo( seqId );
        if( respInfo ) {
            respInfo->recvBuf->resetBuffer( (uint8_t*)msg.data(), msg.size(), apache::thrift::transport::TMemoryBuffer::COPY );
            respInfo->cob();
        }
    }catch( ... ) {
    }
}

void TAsyncDispatchableChannel::stop() {
}

void TAsyncDispatchableChannel::sendFinished( bool success, int32_t seqId ) {
    if( !success ) {
        boost::shared_ptr<RespInfo> respInfo = respInfoMgr_->removeRespInfo( seqId );
        if( respInfo ) {
                respInfo->cob();
        }
    }
}

void TAsyncDispatchableChannel::handleRequestTimeout( int32_t seqId ) {
    boost::shared_ptr<RespInfo> respInfo = respInfoMgr_->removeRespInfo( seqId );

    if( respInfo ) {
        respInfo->cob();
    }

}

int32_t TAsyncDispatchableChannel::extractSeqId( const std::string& msg ) {
	return extractSeqId( (uint8_t*)msg.data(), msg.length() );
}

int32_t TAsyncDispatchableChannel::extractSeqId( uint8_t* buf, uint32_t n ) {
    boost::shared_ptr< apache::thrift::transport::TMemoryBuffer > memBuf( new apache::thrift::transport::TMemoryBuffer( buf, n, apache::thrift::transport::TMemoryBuffer::COPY ) );
    boost::shared_ptr< ::apache::thrift::protocol::TProtocol> prot = protocolFactory_->getProtocol( memBuf );
	int32_t rseqid = 0;
    std::string fname;
    ::apache::thrift::protocol::TMessageType mtype;

    prot->readMessageBegin( fname, mtype, rseqid );

    return rseqid;
}

boost::shared_ptr<apache::thrift::transport::TMemoryBuffer> TAsyncDispatchableChannel::clone( apache::thrift::transport::TMemoryBuffer& buf ) {
    uint8_t* tmpBuf = 0;
	uint32_t sz = 0;

	buf.getBuffer( &tmpBuf, &sz );
	return boost::shared_ptr<apache::thrift::transport::TMemoryBuffer>( new apache::thrift::transport::TMemoryBuffer(tmpBuf, sz, apache::thrift::transport::TMemoryBuffer::COPY ) );
}

void TAsyncDispatchableChannel::copy( apache::thrift::transport::TMemoryBuffer& src, apache::thrift::transport::TMemoryBuffer& dest ) {
    uint8_t* buf = 0;
    uint32_t sz = 0;
    src.getBuffer( &buf, &sz );
    dest.write( buf, sz );
}

}}}//end namespace
