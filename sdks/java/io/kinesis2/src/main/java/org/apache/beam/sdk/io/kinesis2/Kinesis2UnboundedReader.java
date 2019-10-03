package org.apache.beam.sdk.io.kinesis2;

import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Supplier;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Suppliers;
import org.joda.time.Instant;
import software.amazon.awssdk.services.kinesis.model.Record;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardRequest;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardResponseHandler;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayDeque;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class Kinesis2UnboundedReader extends UnboundedSource.UnboundedReader<Record> implements Serializable {

  private final Kinesis2UnboundedSource source;
  private Record current;

  private final Supplier<Queue<Record>> messagesNotYetRead;

  private Instant oldestPendingTimestamp = BoundedWindow.TIMESTAMP_MIN_VALUE;

  public Kinesis2UnboundedReader(Kinesis2UnboundedSource source, Kinesis2CheckpointMark checkpointMark) {
    this.source = source;
    this.current = null;

    this.messagesNotYetRead = Suppliers.memoize((Supplier<Queue<Record>> & Serializable) () ->
        new ArrayDeque<>());
  }

  @Override
  public boolean start() throws IOException {
    subscribeToShard(source.getShardId());
    return advance();
  }

  @Override
  public boolean advance() throws IOException {
    if (messagesNotYetRead.get().isEmpty()) {
      try {
        Thread.sleep(1000);
      } catch (Exception ex) {}
    }

    current = messagesNotYetRead.get().poll();
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

  private void responseHandlerBuilder_VisitorBuilder(
      SubscribeToShardRequest request) throws ExecutionException, TimeoutException, InterruptedException {
    SubscribeToShardResponseHandler.Visitor visitor =
        SubscribeToShardResponseHandler.Visitor.builder()
            .onSubscribeToShardEvent(
                e -> messagesNotYetRead.get().addAll(e.records())
            )
            .build();

    SubscribeToShardResponseHandler responseHandler =
        SubscribeToShardResponseHandler.builder()
            .onError(t -> System.err.println("Error during stream - " + t.getMessage()))
            .subscriber(visitor)
            .build();

    source.getKinesisClient().subscribeToShard(request, responseHandler).join();
  }

  private void subscribeToShard(String shardId) {
    SubscribeToShardRequest request =
        SubscribeToShardRequest.builder()
            .consumerARN(source.getReader().getConsumerArn())
            .shardId(shardId)
            .startingPosition(s -> s.type(source.getReader().getStartingPosition()))
            .build();
    try {
      responseHandlerBuilder_VisitorBuilder(request);
    } catch (Exception ex) {

    }
  }
}
