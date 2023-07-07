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

import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.plan.parser.StatementGenerator;
import org.apache.iotdb.db.queryengine.plan.statement.Statement;
import org.apache.iotdb.tsfile.utils.Pair;

import org.junit.Test;

import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.List;

import static org.apache.iotdb.db.queryengine.plan.expression.Constants.ALIGN_BY_DEVICE_ONLY_SUPPORT_WRITABLE_VIEW;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class AnalyzeViewTest {

  @Test
  public void rawQueryTest() {
    String sql =
        "select s1, ms1, ms1 as another_ms1 from root.cpu.d1, root.cpu.d2 where ms1 > 1 or s2 < 6 align by device";
    Analysis actualAnalysis = analyzeSQL(sql);
    assert actualAnalysis != null;
  }

  @Test
  public void errorSqlTest() {
    List<Pair<String, String>> errorSqlWithMsg =
        Arrays.asList(
            new Pair<>(
                "select ms3 from root.cpu.d3 align by device",
                ALIGN_BY_DEVICE_ONLY_SUPPORT_WRITABLE_VIEW),
            new Pair<>(
                "select s1 from root.cpu.d3 where ms3 > 1 align by device",
                ALIGN_BY_DEVICE_ONLY_SUPPORT_WRITABLE_VIEW),
            new Pair<>(
                "select count(ms3) from root.cpu.d3 having(count(ms3) > 1) align by device",
                ALIGN_BY_DEVICE_ONLY_SUPPORT_WRITABLE_VIEW),
            new Pair<>(
                "select count(ms3) from root.cpu.d3 group by variation(ms3) align by device",
                ALIGN_BY_DEVICE_ONLY_SUPPORT_WRITABLE_VIEW));

    for (Pair<String, String> pair : errorSqlWithMsg) {
      String errorSql = pair.left;
      String errorMsg = pair.right;
      try {
        analyzeSQL(errorSql);
      } catch (SemanticException e) {
        assertEquals("Test sql is: " + errorSql, errorMsg, e.getMessage());
        continue;
      } catch (Exception ex) {
        fail(String.format("Meets exception %s in test sql: `%s`", errorMsg, errorSql));
      }
      fail(String.format("Sql: `%s` must throw exception: %s", errorSql, errorMsg));
    }
  }

  private Analysis analyzeSQL(String sql) {
    Statement statement = StatementGenerator.createStatement(sql, ZonedDateTime.now().getOffset());
    MPPQueryContext context = new MPPQueryContext(new QueryId("test_query"));
    Analyzer analyzer =
        new Analyzer(context, new FakePartitionFetcherImpl(), new FakeSchemaFetcherImpl());
    return analyzer.analyze(statement);
  }
}
