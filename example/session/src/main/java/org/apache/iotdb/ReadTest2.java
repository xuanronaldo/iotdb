/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb;

import org.apache.iotdb.isession.SessionDataSet.DataIterator;
import org.apache.iotdb.isession.pool.SessionDataSetWrapper;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.pool.SessionPool;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

public class ReadTest2 {

  private static SessionPool sessionPool;

  private static final Logger LOGGER = LoggerFactory.getLogger(ReadTest2.class);

  private static int THREAD_NUMBER = 1;

  private static int DEVICE_NUMBER = 20000;

  private static int READ_LOOP = 10000000;

  private static long LOOP_INTERVAL_IN_NS = 3_000_000_000L;

  private static Random r;

  /** Build a custom SessionPool for this example */

  /** Build a redirect-able SessionPool for this example */
  private static void constructRedirectSessionPool() {
    List<String> nodeUrls = new ArrayList<>();
    //    nodeUrls.add("127.0.0.1:6667");
    //    nodeUrls.add("192.168.130.16:6667");
    //    nodeUrls.add("192.168.130.17:6667");
    //    nodeUrls.add("192.168.130.18:6667");
    nodeUrls.add("10.24.58.58:6667");
    nodeUrls.add("10.24.58.67:6667");
    nodeUrls.add("10.24.58.69:6667");
    sessionPool =
        new SessionPool.Builder()
            .nodeUrls(nodeUrls)
            .user("root")
            .password("root")
            .timeOut(180000)
            .maxSize(500)
            .build();
    sessionPool.setFetchSize(10000);
  }

  private static class SyncReadSignal {
    protected volatile boolean needResetLatch = true;
    protected CountDownLatch latch;
    protected long totalCost;
    protected long currentTimestamp;
    protected int count;
    protected String queryName;

    protected SyncReadSignal(int count, String queryName) {
      this.count = count;
      this.queryName = queryName;
    }

    protected void syncCountDownBeforeRead() {
      if (needResetLatch) {
        synchronized (this) {
          if (needResetLatch) {
            latch = new CountDownLatch(this.count);
            needResetLatch = false;
            totalCost = 0L;
            currentTimestamp = System.nanoTime();
          }
        }
      }
    }

    protected void finishReadAndWait(long cost, int loopIndex) throws InterruptedException {
      CountDownLatch currentLatch = latch;
      totalCost += cost;
      synchronized (this) {
        currentLatch.countDown();
        if (currentLatch.getCount() == 0) {
          needResetLatch = true;
          long totalCost = (System.nanoTime() - currentTimestamp);
          LOGGER.info(
              String.format(
                  "[%s][%d] finished with %d thread. AVG COST: %.3fms. TOTAL COST: %.3fms",
                  this.queryName,
                  loopIndex,
                  this.count,
                  this.totalCost * 1.0 / this.count / 1_000_000,
                  totalCost * 1.0 / 1_000_000));
          if (totalCost < LOOP_INTERVAL_IN_NS) {
            Thread.sleep((LOOP_INTERVAL_IN_NS - totalCost) / 1000_000);
          }
        }
      }
      currentLatch.await();
    }
  }

  public static void main(String[] args) throws InterruptedException {
    // Choose the SessionPool you going to use
    constructRedirectSessionPool();

    r = new Random();

    // Run last query
    SyncReadSignal lastQuerySignal =
        new SyncReadSignal(THREAD_NUMBER, "Last Value Query with 1000w");
    Thread[] lastReadThreads = new Thread[THREAD_NUMBER];
    for (int i = 0; i < THREAD_NUMBER; i++) {
      lastReadThreads[i] =
          new Thread(
              new ReaderThread(lastQuerySignal) {
                @Override
                protected void executeQuery()
                    throws IoTDBConnectionException, StatementExecutionException {
                  queryLastValue();
                }
              });
    }
    for (Thread thread : lastReadThreads) {
      thread.start();
    }
  }

  private abstract static class ReaderThread implements Runnable {
    private final SyncReadSignal signal;

    protected ReaderThread(SyncReadSignal signal) {
      this.signal = signal;
    }

    @Override
    public void run() {
      for (int i = 0; i < READ_LOOP; i++) {
        long cost = 10_000_000L;
        signal.syncCountDownBeforeRead();
        try {
          long startTime = System.nanoTime();
          executeQuery();
          cost = System.nanoTime() - startTime;
        } catch (Throwable t) {
          LOGGER.error("error when execute query.", t);
        } finally {
          try {
            signal.finishReadAndWait(cost, i);
          } catch (InterruptedException e) {
            LOGGER.error("error when finish signal.", e);
          }
        }
      }
    }

    protected abstract void executeQuery()
        throws IoTDBConnectionException, StatementExecutionException;
  }

  private static void queryLastValue()
      throws IoTDBConnectionException, StatementExecutionException {
    int device = r.nextInt(DEVICE_NUMBER);
    String sql = "select last(*) from root.test.**";
    executeQuery(sql);
  }

  private static void executeQuery(String sql)
      throws IoTDBConnectionException, StatementExecutionException {
    SessionDataSetWrapper wrapper = null;
    try {
      wrapper = sessionPool.executeQueryStatement(sql);
      // get DataIterator like JDBC
      DataIterator dataIterator = wrapper.iterator();
      while (dataIterator.next()) {
        for (String columnName : wrapper.getColumnNames()) {
          dataIterator.getString(columnName);
        }
      }
    } finally {
      // remember to close data set finally!
      if (wrapper != null) {
        sessionPool.closeResultSet(wrapper);
      }
    }
  }
}
