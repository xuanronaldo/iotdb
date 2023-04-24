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

package org.apache.iotdb.db.mpp.plan.planner.plan.node.write;

import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.mpp.plan.analyze.Analysis;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.WritePlanNode;
import org.apache.iotdb.db.utils.TimePartitionUtils;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FastInsertRowsNode extends InsertRowsNode {
  public FastInsertRowsNode(PlanNodeId id) {
    super(id);
    setAligned(true);
  }

  public FastInsertRowsNode(
      PlanNodeId id,
      List<Integer> insertRowNodeIndexList,
      List<InsertRowNode> fastInsertRowNodeList) {
    super(id, insertRowNodeIndexList, fastInsertRowNodeList);
    setAligned(true);
  }

  public boolean hasFailedMeasurements() {
    return false;
  }

  public void fillValues() throws QueryProcessException {
    for (int i = 0; i < getInsertRowNodeList().size(); i++) {
      ((FastInsertRowNode) getInsertRowNodeList().get(i)).fillValues();
    }
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    PlanNodeType.FAST_INSERT_ROWS.serialize(stream);

    ReadWriteIOUtils.write(insertRowNodeList.size(), stream);

    for (InsertRowNode node : insertRowNodeList) {
      node.subSerialize(stream);
    }
    for (Integer index : insertRowNodeIndexList) {
      ReadWriteIOUtils.write(index, stream);
    }
  }

  public static FastInsertRowsNode deserialize(ByteBuffer byteBuffer) {
    PlanNodeId planNodeId;
    List<InsertRowNode> insertRowNodeList = new ArrayList<>();
    List<Integer> insertRowNodeIndex = new ArrayList<>();

    int size = byteBuffer.getInt();
    for (int i = 0; i < size; i++) {
      FastInsertRowNode insertRowNode = new FastInsertRowNode(new PlanNodeId(""));
      insertRowNode.subDeserialize(byteBuffer);
      insertRowNodeList.add(insertRowNode);
    }
    for (int i = 0; i < size; i++) {
      insertRowNodeIndex.add(byteBuffer.getInt());
    }

    planNodeId = PlanNodeId.deserialize(byteBuffer);
    for (InsertRowNode insertRowNode : insertRowNodeList) {
      insertRowNode.setPlanNodeId(planNodeId);
    }

    FastInsertRowsNode fastInsertRowsNode = new FastInsertRowsNode(planNodeId);
    fastInsertRowsNode.setInsertRowNodeList(insertRowNodeList);
    fastInsertRowsNode.setInsertRowNodeIndexList(insertRowNodeIndex);
    return fastInsertRowsNode;
  }

  @Override
  public List<WritePlanNode> splitByPartition(Analysis analysis) {
    Map<TRegionReplicaSet, InsertRowsNode> splitMap = new HashMap<>();
    for (int i = 0; i < getInsertRowNodeList().size(); i++) {
      InsertRowNode insertRowNode = getInsertRowNodeList().get(i);
      // data region for insert row node
      TRegionReplicaSet dataRegionReplicaSet =
          analysis
              .getDataPartitionInfo()
              .getDataRegionReplicaSetForWriting(
                  insertRowNode.devicePath.getFullPath(),
                  TimePartitionUtils.getTimePartition(insertRowNode.getTime()));
      if (splitMap.containsKey(dataRegionReplicaSet)) {
        InsertRowsNode tmpNode = splitMap.get(dataRegionReplicaSet);
        tmpNode.addOneInsertRowNode(insertRowNode, i);
      } else {
        InsertRowsNode tmpNode = new FastInsertRowsNode(this.getPlanNodeId());
        tmpNode.setDataRegionReplicaSet(dataRegionReplicaSet);
        tmpNode.addOneInsertRowNode(insertRowNode, i);
        splitMap.put(dataRegionReplicaSet, tmpNode);
      }
    }

    return new ArrayList<>(splitMap.values());
  }
}
