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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.extensions.zetasketch;

import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.TypedRead.Method;
import org.apache.beam.sdk.io.gcp.bigquery.SchemaAndRecord;
import org.apache.beam.sdk.io.gcp.testing.BigqueryClient;
import org.apache.beam.sdk.io.gcp.testing.BigqueryMatcher;
import org.apache.beam.sdk.options.ApplicationNameOptions;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestPipelineOptions;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Integration tests for HLL++ sketch compatibility between Beam and BigQuery. The tests verifies
 * that HLL++ sketches created in Beam can be processed by BigQuery, and vice versa.
 */
@RunWith(JUnit4.class)
public class BigQueryHllSketchCompatibilityIT {

  private static final String APP_NAME;
  private static final String PROJECT_ID;
  private static final String DATASET_ID;
  private static final BigqueryClient BIGQUERY_CLIENT;

  private static final List<String> TEST_DATA =
      Arrays.asList("Apple", "Orange", "Banana", "Orange");

  // Data Table: used by testReadSketchFromBigQuery())
  // Schema: only one STRING field named "data".
  // Content: prepopulated with 4 rows: "Apple", "Orange", "Banana", "Orange"
  private static final String DATA_TABLE_ID = "hll_data";
  private static final String DATA_FIELD_NAME = "data";
  private static final String DATA_FIELD_TYPE = "STRING";
  private static final String QUERY_RESULT_FIELD_NAME = "sketch";
  private static final Long EXPECTED_COUNT = 3L;

  // Sketch Table: used by testWriteSketchToBigQuery()
  // Schema: only one BYTES field named "sketch".
  // Content: will be overridden by the sketch computed by the test pipeline each time the test runs
  private static final String SKETCH_TABLE_ID = "hll_sketch";
  private static final String SKETCH_FIELD_NAME = "sketch";
  private static final String SKETCH_FIELD_TYPE = "BYTES";
  // SHA-1 hash of string "[3]", the string representation of a row that has only one field 3 in it
  private static final String EXPECTED_CHECKSUM = "f1e31df9806ce94c5bdbbfff9608324930f4d3f1";

  static {
    ApplicationNameOptions options =
        TestPipeline.testingPipelineOptions().as(ApplicationNameOptions.class);
    APP_NAME = options.getAppName();
    PROJECT_ID = options.as(GcpOptions.class).getProject();
    DATASET_ID = String.format("zetasketch_%tY_%<tm_%<td_%<tH_%<tM_%<tS_%<tL", new Date());
    BIGQUERY_CLIENT = BigqueryClient.getClient(APP_NAME);
  }

  @BeforeClass
  public static void prepareDatasetAndDataTable() throws Exception {
    BIGQUERY_CLIENT.createNewDataset(PROJECT_ID, DATASET_ID);

    // Create Data Table
    TableSchema dataTableSchema =
        new TableSchema()
            .setFields(
                Collections.singletonList(
                    new TableFieldSchema().setName(DATA_FIELD_NAME).setType(DATA_FIELD_TYPE)));
    Table dataTable =
        new Table()
            .setSchema(dataTableSchema)
            .setTableReference(
                new TableReference()
                    .setProjectId(PROJECT_ID)
                    .setDatasetId(DATASET_ID)
                    .setTableId(DATA_TABLE_ID));
    BIGQUERY_CLIENT.createNewTable(PROJECT_ID, DATASET_ID, dataTable);

    // Prepopulate test data to Data Table
    List<Map<String, Object>> rows =
        TEST_DATA.stream()
            .map(v -> Collections.singletonMap(DATA_FIELD_NAME, (Object) v))
            .collect(Collectors.toList());
    BIGQUERY_CLIENT.insertDataToTable(PROJECT_ID, DATASET_ID, DATA_TABLE_ID, rows);
  }

  @AfterClass
  public static void deleteDataset() throws Exception {
    BIGQUERY_CLIENT.deleteDataset(PROJECT_ID, DATASET_ID);
  }

  /**
   * Test that HLL++ sketch computed in BigQuery can be processed by Beam. Hll sketch is computed by
   * {@code HLL_COUNT.INIT} in BigQuery and read into Beam; the test verifies that we can run {@link
   * HllCount.MergePartial} and {@link HllCount.Extract} on the sketch in Beam to get the correct
   * estimated count.
   */
  @Test
  public void testReadSketchFromBigQuery() {
    String tableSpec = String.format("%s.%s", DATASET_ID, DATA_TABLE_ID);
    String query =
        String.format(
            "SELECT HLL_COUNT.INIT(%s) AS %s FROM %s",
            DATA_FIELD_NAME, QUERY_RESULT_FIELD_NAME, tableSpec);
    SerializableFunction<SchemaAndRecord, byte[]> parseQueryResultToByteArray =
        (SchemaAndRecord schemaAndRecord) ->
            // BigQuery BYTES type corresponds to Java java.nio.ByteBuffer type
            ((ByteBuffer) schemaAndRecord.getRecord().get(QUERY_RESULT_FIELD_NAME)).array();

    TestPipelineOptions options =
        TestPipeline.testingPipelineOptions().as(TestPipelineOptions.class);

    Pipeline p = Pipeline.create(options);
    PCollection<Long> result =
        p.apply(
                BigQueryIO.read(parseQueryResultToByteArray)
                    .fromQuery(query)
                    .usingStandardSql()
                    .withMethod(Method.DIRECT_READ)
                    .withCoder(ByteArrayCoder.of()))
            .apply(HllCount.MergePartial.globally()) // no-op, only for testing MergePartial
            .apply(HllCount.Extract.globally());
    PAssert.thatSingleton(result).isEqualTo(EXPECTED_COUNT);
    p.run().waitUntilFinish();
  }

  /**
   * Test that HLL++ sketch computed in Beam can be processed by BigQuery. Hll sketch is computed by
   * {@link HllCount.Init} in Beam and written to BigQuery; the test verifies that we can run {@code
   * HLL_COUNT.EXTRACT()} on the sketch in BigQuery to get the correct estimated count.
   */
  @Test
  public void testWriteSketchToBigQuery() {
    String tableSpec = String.format("%s.%s", DATASET_ID, SKETCH_TABLE_ID);
    String query =
        String.format("SELECT HLL_COUNT.EXTRACT(%s) FROM %s", SKETCH_FIELD_NAME, tableSpec);
    TableSchema tableSchema =
        new TableSchema()
            .setFields(
                Collections.singletonList(
                    new TableFieldSchema().setName(SKETCH_FIELD_NAME).setType(SKETCH_FIELD_TYPE)));

    TestPipelineOptions options =
        TestPipeline.testingPipelineOptions().as(TestPipelineOptions.class);
    // After the pipeline finishes, BigqueryMatcher will send a query to retrieve the estimated
    // count and verifies its correctness using checksum.
    options.setOnSuccessMatcher(
        BigqueryMatcher.createUsingStandardSql(APP_NAME, PROJECT_ID, query, EXPECTED_CHECKSUM));

    Pipeline p = Pipeline.create(options);
    p.apply(Create.of(TEST_DATA))
        .apply(HllCount.Init.forStrings().globally())
        .apply(
            BigQueryIO.<byte[]>write()
                .to(tableSpec)
                .withSchema(tableSchema)
                .withFormatFunction(sketch -> new TableRow().set(SKETCH_FIELD_NAME, sketch))
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE));
    p.run().waitUntilFinish();
  }
}
