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
import org.junit.Rule;
import org.junit.Test;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.Record;
import software.amazon.awssdk.services.kinesis.model.ShardIteratorType;

import java.io.Serializable;

/** Test Coverage for the IO. */
public class KinesisSubscriberTest implements Serializable {
  @Rule
  public final transient TestPipeline pipeline = TestPipeline.create();

  @Test
  public void runSubscribe() {
    pipeline
        .apply(KinesisIO.subscribe()
            .withStreamName("journal-Bills-v2")
            .withConsumerArn("arn:aws:kinesis:us-west-2:795945668521:stream/journal-Bills-v2/consumer/test-beam-subscriber:1569881070")
            .withStartingPosition(ShardIteratorType.TRIM_HORIZON)
            .withKinesisAsyncClientFn((SerializableFunction<Void, KinesisAsyncClient>) input -> {
              KinesisAsyncClient client = KinesisAsyncClient.builder()
                  .httpClientBuilder(NettyNioAsyncHttpClient.builder()
                      .maxConcurrency(2))
                  //.maxPendingConnectionAcquires(10_000)
                  .region(Region.US_WEST_2)
                  //.credentialsProvider(new AWSStaticCredentialsProvider(new BasicAWSCredentials("", "")))
                  .build();
              return client;
            }))
        .apply(ParDo.of(new DoFn<Record, String>() {
          @ProcessElement
          public void processElement(@Element Record record, OutputReceiver<String> out) {
            String data = "";
            try {
              data = new String(record.data().asByteArray(), "UTF-8");
            } catch (Exception ex) {
              System.out.println(ex.getMessage());
            }
            String temp = "Received: " + record.partitionKey() + " - " + record.sequenceNumber() + " - Data: " + data;
            System.out.println(temp);
            out.output(temp);
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
