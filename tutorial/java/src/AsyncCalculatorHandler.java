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

import org.apache.thrift.TException;


import org.apache.thrift.async.AsyncMethodCallback;

// Generated code
import tutorial.*;
import tutorial.Calculator.add_result;
import tutorial.Calculator.calculate_result;
import tutorial.Calculator.ping_result;
import shared.*;
import shared.SharedService.getStruct_result;

import java.util.HashMap;

public class AsyncCalculatorHandler implements Calculator.AsyncServIface {

  private HashMap<Integer,SharedStruct> log;

  public AsyncCalculatorHandler() {
    log = new HashMap<Integer, SharedStruct>();
  }

  public void ping() {
    System.out.println("ping()");
    try {
		Thread.sleep( 1000 );
	} catch (InterruptedException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
  }

  public int add(int n1, int n2) {
    System.out.println("add(" + n1 + "," + n2 + ")");    
    return n1 + n2;
  }

  public int calculate(int logid, Work work) throws InvalidOperation {
    System.out.println("calculate(" + logid + ", {" + work.op + "," + work.num1 + "," + work.num2 + "})");
    int val = 0;
    switch (work.op) {
    case ADD:
      val = work.num1 + work.num2;
      break;
    case SUBTRACT:
      val = work.num1 - work.num2;
      break;
    case MULTIPLY:
      val = work.num1 * work.num2;
      break;
    case DIVIDE:
      if (work.num2 == 0) {
        InvalidOperation io = new InvalidOperation();
        io.what = work.op.getValue();
        io.why = "Cannot divide by 0";
        throw io;
      }
      val = work.num1 / work.num2;
      break;
    default:
      InvalidOperation io = new InvalidOperation();
      io.what = work.op.getValue();
      io.why = "Unknown operation";
      throw io;
    }

    SharedStruct entry = new SharedStruct();
    entry.key = logid;
    entry.value = Integer.toString(val);
    log.put(logid, entry);

    return val;
  }

  public SharedStruct getStruct(int key) {
    System.out.println("getStruct(" + key + ")");
    return log.get(key);
  }

  public void zip() {
    System.out.println("zip()");
  }

@Override
public void getStruct(int key,
		AsyncMethodCallback<getStruct_result> resultHandler) throws TException {
	// TODO Auto-generated method stub
	
}

@Override
public void ping(AsyncMethodCallback<ping_result> resultHandler)
		throws TException {
	System.out.println("ping()");
    try {
		Thread.sleep( 1000 );
	} catch (InterruptedException e) {
		e.printStackTrace();
	}
    resultHandler.onComplete( new ping_result());
	
}

@Override
public void add(int num1, int num2,
		AsyncMethodCallback<add_result> resultHandler) throws TException {
	System.out.println("add(" + num1 + "," + num2 + ")");    
    int sum = num1 + num2;
	
    add_result r = new add_result();
    r.setSuccess(sum);
    
    resultHandler.onComplete(r);
}

@Override
public void calculate(int logid, Work work,
		AsyncMethodCallback<calculate_result> resultHandler) throws TException {
	System.out.println("calculate(" + logid + ", {" + work.op + "," + work.num1 + "," + work.num2 + "})");
	calculate_result r = new calculate_result();
    int val = 0;
    switch (work.op) {
    case ADD:
      val = work.num1 + work.num2;
      break;
    case SUBTRACT:
      val = work.num1 - work.num2;
      break;
    case MULTIPLY:
      val = work.num1 * work.num2;
      break;
    case DIVIDE:
      if (work.num2 == 0) {
        InvalidOperation io = new InvalidOperation();
        io.what = work.op.getValue();
        io.why = "Cannot divide by 0";
        
        r.setOuch( io );        
      } else {
    	  val = work.num1 / work.num2;
      }
      break;
    default:
      InvalidOperation io = new InvalidOperation();
      io.what = work.op.getValue();
      io.why = "Unknown operation";
      r.setOuch( io );
    }

    if( !r.isSetOuch() ) {
    SharedStruct entry = new SharedStruct();
    entry.key = logid;
    entry.value = Integer.toString(val);
    log.put(logid, entry);
    
    
    r.setSuccess( val );
    }

    resultHandler.onComplete(r);
	
}

}


