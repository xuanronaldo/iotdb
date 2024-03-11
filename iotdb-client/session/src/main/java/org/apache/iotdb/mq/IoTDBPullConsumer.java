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

package org.apache.iotdb.mq;

import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.session.Session;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class IoTDBPullConsumer extends IoTDBAbstractConsumer {

  public IoTDBPullConsumer(Builder builder) throws IoTDBConnectionException {
    this.host = builder.host;
    this.port = builder.port;
    this.username = builder.username;
    this.password = builder.password;
    this.groupId = builder.groupId;
    this.consumerId = builder.consumerId;

    session =
        new Session.Builder()
            .host(this.host)
            .port(this.port)
            .username(this.username)
            .password(this.password)
            .build();
    session.open();

    this.topics = new ArrayList<>();
  }

  @Override
  public void subscribe(String topic) {}

  @Override
  public void subscribe(List<String> topics) {}

  @Override
  public void unsubscribe(String topic) {}

  @Override
  public void unsubscribe(List<String> topics) {}

  @Override
  public void unsubscribeAll() {}

  @Override
  public void close() {}

  public static class Builder {
    private String host = IoTDBMQConstant.DEFAULT_HOST;
    private int port = IoTDBMQConstant.DEFAULT_PORT;
    private String username = IoTDBMQConstant.DEFAULT_USERNAME;
    private String password = IoTDBMQConstant.DEFAULT_PASSWORD;
    private String groupId =
        String.format("group_%s", UUID.randomUUID().toString().substring(0, 5));
    private String consumerId =
        String.format("consumer_%s", UUID.randomUUID().toString().substring(0, 5));

    public Builder host(String host) {
      this.host = host;
      return this;
    }

    public Builder port(int port) {
      this.port = port;
      return this;
    }

    public Builder username(String username) {
      this.username = username;
      return this;
    }

    public Builder password(String password) {
      this.password = password;
      return this;
    }

    public Builder groupId(String groupId) {
      this.groupId = groupId;
      return this;
    }

    public Builder consumerId(String consumerId) {
      this.consumerId = consumerId;
      return this;
    }

    public IoTDBPullConsumer build() throws IoTDBConnectionException {
      return new IoTDBPullConsumer(this);
    }
  }
}
