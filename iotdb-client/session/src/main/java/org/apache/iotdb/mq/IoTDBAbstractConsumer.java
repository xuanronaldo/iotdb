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

import org.apache.iotdb.session.Session;

import java.util.List;

public abstract class IoTDBAbstractConsumer {
  protected String host;
  protected int port;
  protected String nodeUrls;
  protected String username;
  protected String password;

  protected String groupId;
  protected String consumerId;

  protected Session session;

  protected List<String> topics;

  public void subscribe(String topic) {
    if (topics.contains(topic)) {
      return;
    }
  }

  public void subscribe(List<String> topics) {}

  public void unsubscribe(String topic) {}

  public void unsubscribe(List<String> topics) {}

  public void unsubscribeAll() {}

  public void close() {}
}
