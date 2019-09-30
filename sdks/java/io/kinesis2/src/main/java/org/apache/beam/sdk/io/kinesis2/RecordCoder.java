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

import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.Coder;
// import org.apache.beam.sdk.coders.InstantCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.kinesis.model.Record;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/** A {@link Coder} for {@link Record}. */
class RecordCoder extends AtomicCoder<Record> {

  private static final StringUtf8Coder STRING_CODER = StringUtf8Coder.of();
  private static final ByteArrayCoder BYTE_ARRAY_CODER = ByteArrayCoder.of();
  //private static final InstantCoder INSTANT_CODER = InstantCoder.of();

  public static RecordCoder of() {
    return new RecordCoder();
  }

  @Override
  public void encode(Record record, OutputStream outStream) throws IOException {
    BYTE_ARRAY_CODER.encode(record.data().asByteArray(), outStream);
    STRING_CODER.encode(record.sequenceNumber(), outStream);
    STRING_CODER.encode(record.partitionKey(), outStream);
    //INSTANT_CODER.encode(record.approximateArrivalTimestamp(), outStream);
  }

  @Override
  public Record decode(InputStream inStream) throws IOException {
    byte[] data = BYTE_ARRAY_CODER.decode(inStream);
    String sequenceNumber = STRING_CODER.decode(inStream);
    String partitionKey = STRING_CODER.decode(inStream);
    //Instant approximateArrivalTimestamp = INSTANT_CODER.decode(inStream);

    return Record.builder()
        .data(SdkBytes.fromByteArray(data))
        .sequenceNumber(sequenceNumber)
        .partitionKey(partitionKey)
        .build();
  }
}
