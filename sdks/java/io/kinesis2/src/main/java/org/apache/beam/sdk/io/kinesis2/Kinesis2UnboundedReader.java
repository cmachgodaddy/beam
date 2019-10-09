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
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class Kinesis2UnboundedReader extends UnboundedSource.UnboundedReader<Record> implements Serializable {

  private final Kinesis2UnboundedSource source;
  private Record current;

  private final Queue<Record> messagesNotYetRead;

  private Instant oldestPendingTimestamp = BoundedWindow.TIMESTAMP_MIN_VALUE;

  public Kinesis2UnboundedReader(Kinesis2UnboundedSource source, Kinesis2CheckpointMark checkpointMark) {
    this.source = source;
    this.current = null;

    this.messagesNotYetRead = //Suppliers.memoize((Supplier<Queue<Record>> & Serializable) () ->
        new ArrayDeque<>();
    subscribeToShard(source.getShardId());
  }

  @Override
  public boolean start() throws IOException {
    return advance();
  }

  @Override
  public boolean advance() throws IOException {
    if (!messagesNotYetRead.isEmpty()) {
      current = messagesNotYetRead.poll();
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

  private CompletableFuture<Void> responseHandlerBuilder_VisitorBuilder(
      SubscribeToShardRequest request) throws ExecutionException, TimeoutException, InterruptedException {
    SubscribeToShardResponseHandler.Visitor visitor =
        SubscribeToShardResponseHandler.Visitor.builder()
            .onSubscribeToShardEvent(
                e -> e.records().forEach(r -> {
                  messagesNotYetRead.add(r);
                })
            )
            .build();

    SubscribeToShardResponseHandler responseHandler =
        SubscribeToShardResponseHandler.builder()
            .onError(t -> System.err.println("Error during stream - " + t.getMessage()))
            .subscriber(visitor)
            .build();

    return source.getKinesisClient().subscribeToShard(request, responseHandler);
  }

  private void subscribeToShard(String shardId) {
    SubscribeToShardRequest request =
        SubscribeToShardRequest.builder()
            .consumerARN(source.getReader().getConsumerArn())
            .shardId(shardId)
            .startingPosition(s -> s.type(source.getReader().getStartingPosition()))
            .build();
    try {
      //while (true) {
        CompletableFuture<Void> future = responseHandlerBuilder_VisitorBuilder(request);
        Thread.sleep(3000);
        future.complete(null);
        //source.getKinesisClient().close();
      //}
    } catch (Exception ex) {

    }
  }
}
