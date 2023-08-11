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

package org.apache.iotdb.db.queryengine.plan.planner.plan.node;

import org.apache.iotdb.db.queryengine.plan.analyze.Analysis;

import java.util.List;

public abstract class WritePlanNode extends PlanNode implements IPartitionRelatedNode {

  /**
   * Whether this node is written by a pipe. If true, this node will be ignored by {@link
   * org.apache.iotdb.db.pipe.extractor.realtime.listener.PipeInsertionDataNodeListener} to avoid
   * cyclic transfer between different clusters.
   */
  private boolean isWrittenByPipe = false;

  protected WritePlanNode(PlanNodeId id) {
    super(id);
  }

  public final List<WritePlanNode> splitByPartition(Analysis analysis) {
    final List<WritePlanNode> nodes = doSplitByPartition(analysis);
    nodes.forEach(node -> node.setIsWrittenByPipe(isWrittenByPipe));
    return nodes;
  }

  protected abstract List<WritePlanNode> doSplitByPartition(Analysis analysis);

  public boolean isWrittenByPipe() {
    return isWrittenByPipe;
  }

  public void setIsWrittenByPipe(boolean isWrittenByPipe) {
    this.isWrittenByPipe = isWrittenByPipe;
  }
}
