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

package org.apache.iotdb.db.queryengine.plan.analyze;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathPatternTree;
import org.apache.iotdb.commons.schema.view.LogicalViewSchema;
import org.apache.iotdb.commons.schema.view.viewExpression.binary.arithmetic.AdditionViewExpression;
import org.apache.iotdb.commons.schema.view.viewExpression.leaf.ConstantViewOperand;
import org.apache.iotdb.commons.schema.view.viewExpression.leaf.TimeSeriesViewOperand;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.schematree.ClusterSchemaTree;
import org.apache.iotdb.db.queryengine.common.schematree.ISchemaTree;
import org.apache.iotdb.db.queryengine.common.schematree.node.SchemaEntityNode;
import org.apache.iotdb.db.queryengine.common.schematree.node.SchemaInternalNode;
import org.apache.iotdb.db.queryengine.common.schematree.node.SchemaMeasurementNode;
import org.apache.iotdb.db.queryengine.common.schematree.node.SchemaNode;
import org.apache.iotdb.db.queryengine.plan.analyze.schema.ISchemaComputationWithAutoCreation;
import org.apache.iotdb.db.queryengine.plan.analyze.schema.ISchemaFetcher;
import org.apache.iotdb.db.schemaengine.template.Template;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

public class FakeSchemaFetcherImpl implements ISchemaFetcher {

  private final ClusterSchemaTree schemaTree = new ClusterSchemaTree(generateSchemaTree());
  private MPPQueryContext context;

  @Override
  public ClusterSchemaTree fetchSchema(PathPatternTree patternTree, MPPQueryContext context) {
    this.context = context;
    schemaTree.setDatabases(new HashSet<>(Arrays.asList("root.sg", "root.cpu")));
    return schemaTree;
  }

  @Override
  public ISchemaTree fetchSchemaWithTags(PathPatternTree patternTree) {
    return fetchSchema(patternTree, context);
  }

  @Override
  public void fetchAndComputeSchemaWithAutoCreate(
      ISchemaComputationWithAutoCreation schemaComputationWithAutoCreation) {}

  @Override
  public void fetchAndComputeSchemaWithAutoCreate(
      List<? extends ISchemaComputationWithAutoCreation> schemaComputationWithAutoCreationList) {
    for (ISchemaComputationWithAutoCreation computation : schemaComputationWithAutoCreationList) {
      computation.computeMeasurement(
          0, new SchemaMeasurementNode("s", new MeasurementSchema("s", TSDataType.INT32)));
    }
  }

  /**
   * Generate the following tree: root.sg.d1.s1, root.sg.d1.s2(status) root.sg.d2.s1,
   * root.sg.d2.s2(status) root.sg.d2.a.s1, root.sg.d2.a.s2(status)
   *
   * @return the root node of the generated schemTree
   */
  private SchemaNode generateSchemaTree() {
    SchemaNode root = new SchemaInternalNode("root");

    SchemaNode sg = new SchemaInternalNode("sg");
    root.addChild("sg", sg);

    SchemaMeasurementNode s1 =
        new SchemaMeasurementNode("s1", new MeasurementSchema("s1", TSDataType.INT32));
    s1.setTagMap(Collections.singletonMap("key1", "value1"));
    SchemaMeasurementNode s2 =
        new SchemaMeasurementNode("s2", new MeasurementSchema("s2", TSDataType.DOUBLE));
    s2.setTagMap(Collections.singletonMap("key1", "value1"));
    SchemaMeasurementNode s3 =
        new SchemaMeasurementNode("s3", new MeasurementSchema("s3", TSDataType.BOOLEAN));
    s3.setTagMap(Collections.singletonMap("key1", "value2"));
    SchemaMeasurementNode s4 =
        new SchemaMeasurementNode("s4", new MeasurementSchema("s4", TSDataType.TEXT));
    s4.setTagMap(Collections.singletonMap("key2", "value1"));
    s2.setAlias("status");

    SchemaEntityNode d1 = new SchemaEntityNode("d1");
    sg.addChild("d1", d1);
    d1.addChild("s1", s1);
    d1.addChild("s2", s2);
    d1.addAliasChild("status", s2);
    d1.addChild("s3", s3);

    SchemaEntityNode d2 = new SchemaEntityNode("d2");
    sg.addChild("d2", d2);
    d2.addChild("s1", s1);
    d2.addChild("s2", s2);
    d2.addAliasChild("status", s2);
    d2.addChild("s4", s4);

    SchemaEntityNode a = new SchemaEntityNode("a");
    a.setAligned(true);
    d2.addChild("a", a);
    a.addChild("s1", s1);
    a.addChild("s2", s2);
    a.addAliasChild("status", s2);

    // add view node
    SchemaNode cpu = new SchemaInternalNode("cpu");
    root.addChild("cpu", cpu);

    // device1 only has base series (s1, s2, ms1)
    SchemaEntityNode cpuDevice1 = new SchemaEntityNode("d1");
    cpuDevice1.addChild(
        "s1", new SchemaMeasurementNode("s1", new MeasurementSchema("s1", TSDataType.INT32)));
    cpuDevice1.addChild(
        "s2", new SchemaMeasurementNode("s2", new MeasurementSchema("s2", TSDataType.FLOAT)));
    cpuDevice1.addChild(
        "ms1", new SchemaMeasurementNode("ms1", new MeasurementSchema("ms1", TSDataType.INT32)));

    // device1 only has base series (s1, s2)
    SchemaEntityNode cpuDevice2 = new SchemaEntityNode("d2");
    cpuDevice2.addChild(
        "s1", new SchemaMeasurementNode("s1", new MeasurementSchema("s1", TSDataType.INT32)));
    cpuDevice2.addChild(
        "s2", new SchemaMeasurementNode("s2", new MeasurementSchema("s2", TSDataType.FLOAT)));

    // device3 only has view series (ms1, ms2, ms3)
    SchemaEntityNode cpuDevice3 = new SchemaEntityNode("d3");
    cpuDevice3.addChild(
        "ms1",
        new SchemaMeasurementNode(
            "ms1", new LogicalViewSchema("ms1", new TimeSeriesViewOperand("root.cpu.d1.s1"))));
    cpuDevice3.addChild(
        "ms2",
        new SchemaMeasurementNode(
            "ms2", new LogicalViewSchema("ms2", new TimeSeriesViewOperand("root.cpu.d1.s1"))));
    TimeSeriesViewOperand timeSeriesViewOperand = new TimeSeriesViewOperand("root.cpu.d1.s1");
    ConstantViewOperand constantViewOperand = new ConstantViewOperand(TSDataType.INT32, "2");
    AdditionViewExpression add =
        new AdditionViewExpression(timeSeriesViewOperand, constantViewOperand);
    cpuDevice3.addChild("ms3", new SchemaMeasurementNode("ms3", new LogicalViewSchema("ms3", add)));

    // device4 has both base series(s1) and view series(ms1)
    SchemaEntityNode cpuDevice4 = new SchemaEntityNode("d4");
    cpuDevice4.addChild(
        "s1", new SchemaMeasurementNode("s1", new MeasurementSchema("s1", TSDataType.INT32)));
    cpuDevice4.addChild(
        "ms1",
        new SchemaMeasurementNode(
            "ms1", new LogicalViewSchema("ms1", new TimeSeriesViewOperand("root.cpu.d2.s1"))));

    cpu.addChild("d1", cpuDevice1);
    cpu.addChild("d2", cpuDevice2);
    cpu.addChild("d3", cpuDevice3);
    cpu.addChild("d4", cpuDevice4);

    return root;
  }

  @Override
  public ISchemaTree fetchSchemaListWithAutoCreate(
      List<PartialPath> devicePath,
      List<String[]> measurements,
      List<TSDataType[]> tsDataTypes,
      List<TSEncoding[]> encodings,
      List<CompressionType[]> compressionTypes,
      List<Boolean> aligned) {
    return null;
  }

  @Override
  public Pair<Template, PartialPath> checkTemplateSetInfo(PartialPath devicePath) {
    return null;
  }

  @Override
  public Pair<Template, PartialPath> checkTemplateSetAndPreSetInfo(
      PartialPath timeSeriesPath, String alias) {
    return null;
  }

  @Override
  public Map<Integer, Template> checkAllRelatedTemplate(PartialPath pathPattern) {
    return Collections.emptyMap();
  }

  @Override
  public Pair<Template, List<PartialPath>> getAllPathsSetTemplate(String templateName) {
    return null;
  }
}
