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

package org.apache.thrift.transport;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.IOException;

import java.net.URL;
import java.util.HashMap;
import java.util.Map;

import org.apache.hc.client5.http.fluent.Request;

import org.apache.hc.client5.http.fluent.Response;
import org.apache.hc.core5.util.Timeout;

/**
 * HTTP implementation of the TTransport interface. Used for working with a
 * Thrift web services implementation (using for example TServlet).
 *
 * This class offers two implementations of the HTTP transport.
 * One uses HttpURLConnection instances, the other HttpClient from Apache
 * Http Components.
 * The chosen implementation depends on the constructor used to
 * create the THttpClient instance.
 * Using the THttpClient(String url) constructor or passing null as the
 * HttpClient to THttpClient(String url, HttpClient client) will create an
 * instance which will use HttpURLConnection.
 *
 * When using HttpClient, the following configuration leads to 5-15% 
 * better performance than the HttpURLConnection implementation:
 *
 * http.protocol.version=HttpVersion.HTTP_1_1
 * http.protocol.content-charset=UTF-8
 * http.protocol.expect-continue=false
 * http.connection.stalecheck=false
 *
 * Also note that under high load, the HttpURLConnection implementation
 * may exhaust the open file descriptor limit.
 *
 * @see <a href="https://issues.apache.org/jira/browse/THRIFT-970">THRIFT-970</a>
 */

public class THttpClient extends TTransport {

  private URL url_ = null;

  private final ByteArrayOutputStream requestBuffer_ = new ByteArrayOutputStream();

  private InputStream inputStream_ = null;

  private int connectTimeout_ = 0;

  private int readTimeout_ = 0;

  private Map<String,String> customHeaders_ = null;


  public static class Factory extends TTransportFactory {
    
    private final String url;

    public Factory(String url) {
      this.url = url;
    }


    @Override
    public TTransport getTransport(TTransport trans) {
      try {
          return new THttpClient(url);
      } catch (TTransportException tte) {
        return null;
      }
    }
  }

  public THttpClient(String url) throws TTransportException {
    try {
      url_ = new URL(url);
    } catch (IOException iox) {
      throw new TTransportException(iox);
    }
  }


  public void setConnectTimeout(int timeout) {
    connectTimeout_ = timeout;
  }

  public void setReadTimeout(int timeout) {
    readTimeout_ = timeout;
  }

  public void setCustomHeaders(Map<String,String> headers) {
    customHeaders_ = headers;
  }

  public void setCustomHeader(String key, String value) {
    if (customHeaders_ == null) {
      customHeaders_ = new HashMap<String, String>();
    }
    customHeaders_.put(key, value);
  }

  public void open() {}

  public void close() {
    if (null != inputStream_) {
      try {
        inputStream_.close();
      } catch (IOException ioe) {
        ;
      }
      inputStream_ = null;
    }
  }

  public boolean isOpen() {
    return true;
  }

  public int read(byte[] buf, int off, int len) throws TTransportException {
    if (inputStream_ == null) {
      throw new TTransportException("Response buffer is empty, no request.");
    }
    try {
      int ret = inputStream_.read(buf, off, len);
      if (ret == -1) {
        throw new TTransportException("No more data available.");
      }
      return ret;
    } catch (IOException iox) {
      throw new TTransportException(iox);
    }
  }

  public void write(byte[] buf, int off, int len) {
    requestBuffer_.write(buf, off, len);
  }

  private void flushUsingHttpClient() throws TTransportException {
    

    // Extract request and reset buffer
    byte[] data = requestBuffer_.toByteArray();
    requestBuffer_.reset();


    InputStream is = null;
    
    try {
      // Set request to path + query string
      Request post = Request.post(this.url_.getFile());

      //
      // Headers are added to the HttpPost instance, not
      // to HttpClient.
      //

      post.setHeader("Content-Type", "application/x-thrift");
      post.setHeader("Accept", "application/x-thrift");
      post.setHeader("User-Agent", "Java/THttpClient/HC");
      post.connectTimeout(Timeout.ofSeconds(this.connectTimeout_));
      post.responseTimeout(Timeout.ofSeconds(this.readTimeout_));

      if (null != customHeaders_) {
        for (Map.Entry<String, String> header : customHeaders_.entrySet()) {
          post.setHeader(header.getKey(), header.getValue());
        }
      }

      post.bodyByteArray(data);

      Response response = post.execute();
      int responseCode = response.returnResponse().getCode();

      //
      // Retrieve the inputstream BEFORE checking the status code so
      // resources get freed in the finally clause.
      //

      is = response.returnContent().asStream();

      if (responseCode != 200 ) {
        throw new TTransportException("HTTP Response code: " + responseCode);
      }

      // Read the responses into a byte array so we can release the connection
      // early. This implies that the whole content will have to be read in
      // memory, and that momentarily we might use up twice the memory (while the
      // thrift struct is being read up the chain).
      // Proceeding differently might lead to exhaustion of connections and thus
      // to app failure.

      byte[] buf = new byte[1024];
      ByteArrayOutputStream baos = new ByteArrayOutputStream();

      int len = 0;
      do {
        len = is.read(buf);
        if (len > 0) {
          baos.write(buf, 0, len);
        }
      } while (-1 != len);

      inputStream_ = new ByteArrayInputStream(baos.toByteArray());

    } catch (IOException ioe) {
      // Abort method so the connection gets released back to the connection manager
      throw new TTransportException(ioe);
    } finally {
      if (null != is) {
        // Close the entity's input stream, this will release the underlying connection
        try {
          is.close();
        } catch (IOException ioe) {
          throw new TTransportException(ioe);
        }
      }
    }
  }

  public void flush() throws TTransportException {
      flushUsingHttpClient();
  }
}
