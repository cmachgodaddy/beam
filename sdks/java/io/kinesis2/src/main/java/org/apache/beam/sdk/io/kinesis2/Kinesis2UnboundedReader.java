package org.apache.beam.sdk.io.kinesis2;

import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.joda.time.Instant;
import software.amazon.awssdk.services.kinesis.model.Record;

import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.NoSuchElementException;

public class Kinesis2UnboundedReader extends UnboundedSource.UnboundedReader<Record> implements Serializable {

  private final Kinesis2UnboundedSource source;
  private Record current;

  private Instant oldestPendingTimestamp = BoundedWindow.TIMESTAMP_MIN_VALUE;

  public Kinesis2UnboundedReader(Kinesis2UnboundedSource source, Kinesis2CheckpointMark checkpointMark) {
    this.source = source;
    this.current = null;
  }

  @Override
  public boolean start() {
    System.out.println("ShardId: " + source.getShardId());
    return advance();
  }

  @Override
  public boolean advance() {
    if (!source.getMessagesToRead().get().isEmpty()) {
      current = source.getMessagesToRead().get().poll();
    }

    if (current == null) {
      return false;
    }

    Instant currentMessageTimestamp = getCurrentTimestamp();
    if (getCurrentTimestamp().isBefore(oldestPendingTimestamp)) {
      oldestPendingTimestamp = currentMessageTimestamp;
    }

    return true;
  }

  @Override
  public Record getCurrent() throws NoSuchElementException {
    if (current == null) {
      throw new NoSuchElementException();
    }
    return current;
  }

  @Override
  public byte[] getCurrentRecordId() throws NoSuchElementException {
    if (current == null) {
      throw new NoSuchElementException();
    }
    return current.sequenceNumber().getBytes(StandardCharsets.UTF_8);
  }

  @Override
  public Instant getCurrentTimestamp() throws NoSuchElementException {
    if (current == null) {
      throw new NoSuchElementException();
    }

    return new Instant(current.approximateArrivalTimestamp().getEpochSecond());
  }

  @Override
  public void close() throws IOException {

  }

  @Override
  public Instant getWatermark() {
    return oldestPendingTimestamp;
  }

  @Override
  public UnboundedSource.CheckpointMark getCheckpointMark() {
    return new Kinesis2CheckpointMark(this);
  }

  @Override
  public UnboundedSource<Record, ?> getCurrentSource() {
    return source;
  }
}
