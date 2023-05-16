package org.apache.iotdb;

import org.apache.iotdb.common.rpc.thrift.TAggregationType;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.pool.SessionPool;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class TsbsExample {

  private static SessionPool sessionPool;

  private static final Logger LOGGER = LoggerFactory.getLogger(ReadTest.class);

  private static int THREAD_NUMBER = 300;

  private static int DEVICE_NUMBER = 20000;

  private static int READ_LOOP = 10000000;

  private static void constructRedirectSessionPool() {
    List<String> nodeUrls = new ArrayList<>();
    nodeUrls.add("127.0.0.1:6667");
    sessionPool =
        new SessionPool.Builder()
            .nodeUrls(nodeUrls)
            .user("root")
            .password("root")
            .maxSize(500)
            .build();
    sessionPool.setFetchSize(10000);
  }

  private static void executeQuery() throws IoTDBConnectionException, StatementExecutionException {
    List<String> paths = Collections.singletonList("root.sg.cpu.host_931.usage_user");
    List<TAggregationType> types = Collections.singletonList(TAggregationType.MAX_VALUE);
    sessionPool.executeAggregationQuery(paths, types, 1451691893000L, 1451695493000L, 60000);
  }

  public static void main(String[] args)
      throws IoTDBConnectionException, StatementExecutionException {
    constructRedirectSessionPool();
    executeQuery();
  }
}
