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

package org.apache.iotdb.db.pipe.connector.protocol.thrift;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeInsertNodeTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeRawTabletInsertionEvent;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertNode;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class LeaderCacheManager {
  private final Map<String, TEndPoint> device2endpoint = new ConcurrentHashMap<>();

  public TEndPoint getEndPointByDeviceId(String deviceId) {
    return device2endpoint.getOrDefault(deviceId, null);
  }

  public TEndPoint parseEndPoint(PipeInsertNodeTabletInsertionEvent event) {
    InsertNode insertNode = event.getInsertNodeViaCacheIfPossible();
    return insertNode == null
        ? null
        : getEndPointByDeviceId(insertNode.getDevicePath().getFullPath());
  }

  public TEndPoint parseEndPoint(PipeRawTabletInsertionEvent event) {
    return getEndPointByDeviceId(event.getDeviceId());
  }

  public void updateOrCreate(String deviceId, TEndPoint endPoint) {
    if (device2endpoint.containsKey(deviceId)) {
      device2endpoint.replace(deviceId, endPoint);
    } else {
      device2endpoint.put(deviceId, endPoint);
    }
  }
}
