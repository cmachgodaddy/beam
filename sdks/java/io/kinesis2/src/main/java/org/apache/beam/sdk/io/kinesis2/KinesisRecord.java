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

import static org.apache.commons.lang.builder.HashCodeBuilder.reflectionHashCode;

import com.amazonaws.services.kinesis.clientlibrary.types.ExtendedSequenceNumber;
import software.amazon.awssdk.services.kinesis.model.Record;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.joda.time.Instant;

/** {@link Record} enhanced with utility methods. */
public class KinesisRecord {

  private Instant readTime;
  private String streamName;
  private String shardId;
  private long subSequenceNumber;
  private String sequenceNumber;
  private Instant approximateArrivalTimestamp;
  private ByteBuffer data;
  private String partitionKey;

  public KinesisRecord(Record record, String streamName, String shardId) {
    this(
        record.data().asByteBuffer(),
        record.sequenceNumber(),
        4l, // record.sequenceNumber(),
        record.partitionKey(),
        new Instant(record.approximateArrivalTimestamp()),
        Instant.now(),
        streamName,
        shardId);
  }

  public KinesisRecord(
      ByteBuffer data,
      String sequenceNumber,
      long subSequenceNumber,
      String partitionKey,
      Instant approximateArrivalTimestamp,
      Instant readTime,
      String streamName,
      String shardId) {
    this.data = data;
    this.sequenceNumber = sequenceNumber;
    this.subSequenceNumber = subSequenceNumber;
    this.partitionKey = partitionKey;
    this.approximateArrivalTimestamp = approximateArrivalTimestamp;
    this.readTime = readTime;
    this.streamName = streamName;
    this.shardId = shardId;
  }

  public ExtendedSequenceNumber getExtendedSequenceNumber() {
    return new ExtendedSequenceNumber(getSequenceNumber(), getSubSequenceNumber());
  }

  /** @return The unique identifier of the record based on its position in the stream. */
  public byte[] getUniqueId() {
    return getExtendedSequenceNumber().toString().getBytes(StandardCharsets.UTF_8);
  }

  public Instant getReadTime() {
    return readTime;
  }

  public String getStreamName() {
    return streamName;
  }

  public String getShardId() {
    return shardId;
  }

  public byte[] getDataAsBytes() {
    return getData().array();
  }

  @Override
  public boolean equals(Object obj) {
    return EqualsBuilder.reflectionEquals(this, obj);
  }

  @Override
  public int hashCode() {
    return reflectionHashCode(this);
  }

  public long getSubSequenceNumber() {
    return subSequenceNumber;
  }

  /** @return The unique identifier of the record within its shard. */
  public String getSequenceNumber() {
    return sequenceNumber;
  }

  /** @return The approximate time that the record was inserted into the stream. */
  public Instant getApproximateArrivalTimestamp() {
    return approximateArrivalTimestamp;
  }

  /** @return The data blob. */
  public ByteBuffer getData() {
    return data;
  }

  public String getPartitionKey() {
    return partitionKey;
  }
}
