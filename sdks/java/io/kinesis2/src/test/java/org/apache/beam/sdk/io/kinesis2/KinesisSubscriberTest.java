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

package org.apache.beam.sdk.io.kinesis2;

import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import org.junit.Rule;
import org.junit.Test;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.Record;
import software.amazon.awssdk.services.kinesis.model.ShardIteratorType;

import java.io.Serializable;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/** Test Coverage for the IO. */
public class KinesisSubscriberTest implements Serializable {
  @Rule
  public final transient TestPipeline pipeline = TestPipeline.create();

  @Test
  public void runSubscribe() {
    TupleTag<CompletableFuture<Void>> futureTupleTag = new TupleTag<>();

    PCollection<List<Record>> listOfRecords = pipeline
        .apply(KinesisIO.subscribe()
            .withStreamName("yourstreamname")
            .withConsumerArn("your consume arn")
            .withStartingPosition(ShardIteratorType.TRIM_HORIZON)
            .withCompletableFutureTag(futureTupleTag)
            .withKinesisAsyncClientFn((SerializableFunction<Void, KinesisAsyncClient>) input -> {
              KinesisAsyncClient client = KinesisAsyncClient.builder()
                  .httpClientBuilder(NettyNioAsyncHttpClient.builder()
                      .maxConcurrency(2))
                  //.maxPendingConnectionAcquires(10_000)
                  .region(Region.US_WEST_2)
                  //.credentialsProvider(new AWSStaticCredentialsProvider(new BasicAWSCredentials("", "")))
                  .build();
              return client;
            }).items());

    listOfRecords
        .apply(ParDo.of(new DoFn<List<Record>, String>() {
          @ProcessElement
          public void processElement(@Element List<Record> records, OutputReceiver<String> out) {
            records.forEach(r -> {
              String data = "";
              try {
                data = new String(r.data().asByteArray(), "UTF-8");
              } catch (Exception ex) {
                System.out.println(ex.getMessage());
              }
              String temp = "Received: " + r.partitionKey() + " - " + r.sequenceNumber() + " - Data: " + data;
              out.output(temp);
              System.out.println(temp);
            });
          }
        }));

    //.apply(MapElements.into(TypeDescriptor.of(Record.class)).via(record -> record.toString()))
        /*.apply(Window.<String>into(FixedWindows.of(org.joda.time.Duration.millis(5000)))
            .triggering(Repeatedly.forever(AfterProcessingTime.pastFirstElementInPane()
                .plusDelayOf(org.joda.time.Duration.millis(1000))))
            .withAllowedLateness(org.joda.time.Duration.millis(1000)).discardingFiredPanes())
        .apply(TextIO.write().to("file:///tmp/bill_unified").withNumShards(1).withWindowedWrites());*/

    pipeline.run().waitUntilFinish();
  }
}
