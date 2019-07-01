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
package org.apache.beam.sdk.io.aws.dynamodb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.testing.ExpectedLogs;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.ImmutableMap;
import org.joda.time.Duration;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;
import software.amazon.awssdk.services.dynamodb.model.PutRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.WriteRequest;

/** Test Coverage for the IO. */
public class DynamoDbIoTest implements Serializable {
  @Rule public final transient TestPipeline pipeline = TestPipeline.create();
  @Rule public final transient ExpectedLogs expectedLogs = ExpectedLogs.none(DynamoDbIo.class);

  private static final String tableName = "TaskA";
  private static final int numOfItems = 10;

  private static List<Map<String, AttributeValue>> expected;

  @BeforeClass
  public static void setup() {
    DynamoDbIoTestHelper.startServerClient();
    DynamoDbIoTestHelper.createTestTable(tableName);
    expected = DynamoDbIoTestHelper.generateTestData(tableName, numOfItems);
  }

  @AfterClass
  public static void destroy() {
    DynamoDbIoTestHelper.stopServerClient(tableName);
  }

  // Test cases for Reader.
  @Test
  public void testReadScanResult() {
    PCollection<List<Map<String, AttributeValue>>> actual =
        pipeline.apply(
            DynamoDbIo.<List<Map<String, AttributeValue>>>read()
                .withAwsClientsProvider(
                    AwsClientsProviderMock.of(DynamoDbIoTestHelper.getDynamoDBClient()))
                .withScanRequestFn(
                    (SerializableFunction<Void, ScanRequest>)
                        input ->
                            ScanRequest.builder().tableName(tableName).totalSegments(1).build())
                .items());
    PAssert.that(actual).containsInAnyOrder(expected);
    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testReadScanResult_v2() {
    PCollection<List<Map<String, AttributeValue>>> actual =
        pipeline.apply(
            DynamoDbIo.<List<Map<String, AttributeValue>>>read()
                .withAwsClientsProvider(
                    AwsClientsProviderMock.of(DynamoDbIoTestHelper.getDynamoDBClient()))
                .withScanRequestFn(
                    (SerializableFunction<Void, ScanRequest>)
                        input ->
                            ScanRequest.builder().tableName(tableName).totalSegments(3).build())
                .items())
        .apply(ParDo.of(new DoFn<List<Map<String, AttributeValue>>, List<Map<String, AttributeValue>>>() {
          @ProcessElement
          public void processElement(@Element List<Map<String, AttributeValue>> input, OutputReceiver<List<Map<String, AttributeValue>>> out) {
            out.output(input);
          }
        }));
    PAssert.that(actual).containsInAnyOrder(expected);
    pipeline.run().waitUntilFinish();
  }

  // Test cases for Reader's arguments.
  @Test
  public void testMissingScanRequestFn() {
    thrown.expectMessage("withScanRequestFn() is required");
    pipeline.apply(
        DynamoDbIo.read()
            .withAwsClientsProvider(
                AwsClientsProviderMock.of(DynamoDbIoTestHelper.getDynamoDBClient())));
    try {
      pipeline.run().waitUntilFinish();
      fail("withScanRequestFn() is required");
    } catch (IllegalArgumentException ex) {
      assertEquals("withScanRequestFn() is required", ex.getMessage());
    }
  }

  @Test
  public void testMissingAwsClientsProvider() {
    thrown.expectMessage("withAwsClientsProvider() is required");
    pipeline.apply(
        DynamoDbIo.read()
            .withScanRequestFn(
                (SerializableFunction<Void, ScanRequest>)
                    input -> ScanRequest.builder().tableName(tableName).totalSegments(3).build()));
    try {
      pipeline.run().waitUntilFinish();
      fail("withAwsClientsProvider() is required");
    } catch (IllegalArgumentException ex) {
      assertEquals("withAwsClientsProvider() is required", ex.getMessage());
    }
  }

  @Test
  public void testMissingTotalSegments() {
    thrown.expectMessage("TotalSegments is required with withScanRequestFn()");
    pipeline.apply(
        DynamoDbIo.read()
            .withScanRequestFn(
                (SerializableFunction<Void, ScanRequest>)
                    input -> ScanRequest.builder().tableName(tableName).build())
            .withAwsClientsProvider(
                AwsClientsProviderMock.of(DynamoDbIoTestHelper.getDynamoDBClient())));
    try {
      pipeline.run().waitUntilFinish();
      fail("TotalSegments is required with withScanRequestFn()");
    } catch (IllegalArgumentException ex) {
      assertEquals("TotalSegments is required with withScanRequestFn()", ex.getMessage());
    }
  }

  @Test
  public void testNegativeTotalSegments() {
    thrown.expectMessage("TotalSegments is required with withScanRequestFn() and greater zero");
    pipeline.apply(
        DynamoDbIo.read()
            .withScanRequestFn(
                (SerializableFunction<Void, ScanRequest>)
                    input -> ScanRequest.builder().tableName(tableName).totalSegments(-1).build())
            .withAwsClientsProvider(
                AwsClientsProviderMock.of(DynamoDbIoTestHelper.getDynamoDBClient())));
    try {
      pipeline.run().waitUntilFinish();
      fail("withTotalSegments() is expected and greater than zero");
    } catch (IllegalArgumentException ex) {
      assertEquals(
          "TotalSegments is required with withScanRequestFn() and greater zero", ex.getMessage());
    }
  }

  @Test
  public void testWriteDataToDynamo_v2() {
    List<KV<String, Integer>> items = ImmutableList.of(KV.of("test1", 111), KV.of("test2", 222), KV.of("test3", 333));

    final PCollection<Void> output =
        pipeline
            .apply(Create.of(items))
            .apply(
                DynamoDbIo.<KV<String, Integer>>write()
                    .withWriteRequestMapperFn(
                        (SerializableFunction<KV<String, Integer>, KV<String, WriteRequest>>)
                            entry -> {
                              Map<String, AttributeValue> putRequest = ImmutableMap.of(
                                  "hashKey1", AttributeValue.builder().s(entry.getKey()).build(),
                                  "rangeKey2", AttributeValue.builder().n(entry.getValue().toString()).build());

                              WriteRequest writeRequest = WriteRequest.builder().putRequest(
                                  PutRequest.builder().item(putRequest).build())
                                  .build();
                              return KV.of(tableName, writeRequest);
                            })
                    .withRetryConfiguration(
                        DynamoDbIo.RetryConfiguration.create(5, Duration.standardMinutes(1)))
                    .withAwsClientsProvider(
                        AwsClientsProviderMock.of(DynamoDbIoTestHelper.getDynamoDBClient())));

    final PCollection<Long> publishedResultsSize = output.apply(Count.globally());
    PAssert.that(publishedResultsSize).containsInAnyOrder(0L);

    pipeline.run().waitUntilFinish();
  }

  // Test cases for Writer.
  @Test
  public void testWriteDataToDynamo() {
    final List<WriteRequest> writeRequests = DynamoDbIoTestHelper.generateWriteRequests(numOfItems);

    final PCollection<Void> output =
        pipeline
            .apply(Create.of(writeRequests))
            .apply(
                DynamoDbIo.<WriteRequest>write()
                    .withWriteRequestMapperFn(
                        (SerializableFunction<WriteRequest, KV<String, WriteRequest>>)
                            writeRequest -> KV.of(tableName, writeRequest))
                    .withRetryConfiguration(
                        DynamoDbIo.RetryConfiguration.create(5, Duration.standardMinutes(1)))
                    .withAwsClientsProvider(
                        AwsClientsProviderMock.of(DynamoDbIoTestHelper.getDynamoDBClient())));

    final PCollection<Long> publishedResultsSize = output.apply(Count.globally());
    PAssert.that(publishedResultsSize).containsInAnyOrder(0L);

    pipeline.run().waitUntilFinish();
  }

  @Rule public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testRetries_v2() throws Throwable {
    thrown.expectMessage("Error writing to DynamoDB");

    List<KV<String, Integer>> items = ImmutableList.of(KV.of("test1", 111), KV.of("test2", 222), KV.of("test3", 333));

    DynamoDbClient amazonDynamoDBMock = Mockito.mock(DynamoDbClient.class);
    Mockito.when(amazonDynamoDBMock.batchWriteItem(Mockito.any(BatchWriteItemRequest.class)))
        .thenThrow(DynamoDbException.builder().message("Service unavailable").build());

    pipeline
        .apply(Create.of(items))
        .apply(
            DynamoDbIo.<KV<String, Integer>>write()
                .withWriteRequestMapperFn(
                    (SerializableFunction<KV<String, Integer>, KV<String, WriteRequest>>)
                        entry -> {
                          Map<String, AttributeValue> putRequest = ImmutableMap.of(
                              "hashKey1", AttributeValue.builder().s(entry.getKey()).build(),
                              "rangeKey2", AttributeValue.builder().n(entry.getValue().toString()).build());

                          WriteRequest writeRequest = WriteRequest.builder().putRequest(
                              PutRequest.builder().item(putRequest).build())
                              .build();
                          return KV.of(tableName, writeRequest);
                        })
                .withRetryConfiguration(
                    DynamoDbIo.RetryConfiguration.create(4, Duration.standardSeconds(10)))
                .withAwsClientsProvider(AwsClientsProviderMock.of(amazonDynamoDBMock)));

    try {
      pipeline.run().waitUntilFinish();
    } catch (final Pipeline.PipelineExecutionException e) {
      // check 3 retries were initiated by inspecting the log before passing on the exception
      expectedLogs.verifyWarn(String.format(DynamoDbIo.Write.WriteFn.RETRY_ATTEMPT_LOG, 1));
      expectedLogs.verifyWarn(String.format(DynamoDbIo.Write.WriteFn.RETRY_ATTEMPT_LOG, 2));
      expectedLogs.verifyWarn(String.format(DynamoDbIo.Write.WriteFn.RETRY_ATTEMPT_LOG, 3));
      throw e.getCause();
    }
    fail("Pipeline is expected to fail because we were unable to write to DynamoDB.");
  }

  @Test
  public void testRetries() throws Throwable {
    thrown.expectMessage("Error writing to DynamoDB");

    final List<WriteRequest> writeRequests = DynamoDbIoTestHelper.generateWriteRequests(numOfItems);

    DynamoDbClient amazonDynamoDBMock = Mockito.mock(DynamoDbClient.class);
    Mockito.when(amazonDynamoDBMock.batchWriteItem(Mockito.any(BatchWriteItemRequest.class)))
        .thenThrow(DynamoDbException.builder().message("Service unavailable").build());

    pipeline
        .apply(Create.of(writeRequests))
        .apply(
            DynamoDbIo.<WriteRequest>write()
                .withWriteRequestMapperFn(
                    (SerializableFunction<WriteRequest, KV<String, WriteRequest>>)
                        writeRequest -> KV.of(tableName, writeRequest))
                .withRetryConfiguration(
                    DynamoDbIo.RetryConfiguration.create(4, Duration.standardSeconds(10)))
                .withAwsClientsProvider(AwsClientsProviderMock.of(amazonDynamoDBMock)));

    try {
      pipeline.run().waitUntilFinish();
    } catch (final Pipeline.PipelineExecutionException e) {
      // check 3 retries were initiated by inspecting the log before passing on the exception
      expectedLogs.verifyWarn(String.format(DynamoDbIo.Write.WriteFn.RETRY_ATTEMPT_LOG, 1));
      expectedLogs.verifyWarn(String.format(DynamoDbIo.Write.WriteFn.RETRY_ATTEMPT_LOG, 2));
      expectedLogs.verifyWarn(String.format(DynamoDbIo.Write.WriteFn.RETRY_ATTEMPT_LOG, 3));
      throw e.getCause();
    }
    fail("Pipeline is expected to fail because we were unable to write to DynamoDB.");
  }
}
