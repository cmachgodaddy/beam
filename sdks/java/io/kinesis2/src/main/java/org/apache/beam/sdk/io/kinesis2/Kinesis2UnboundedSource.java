package org.apache.beam.sdk.io.kinesis2;

import com.google.errorprone.annotations.CanIgnoreReturnValue;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Supplier;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Suppliers;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.ListShardsRequest;
import software.amazon.awssdk.services.kinesis.model.ListShardsResponse;
import software.amazon.awssdk.services.kinesis.model.Record;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardRequest;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardResponseHandler;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import javax.annotation.Nullable;

public class Kinesis2UnboundedSource extends UnboundedSource<Record, Kinesis2CheckpointMark> {

  private final String shardId;
  private final KinesisIO.Subscribe read;
  private final Supplier<KinesisAsyncClient> kinesisClient;
  private final Supplier<Queue<Record>> messagesToRead;

  public Kinesis2UnboundedSource(String shardId, Supplier<Queue<Record>> messagesToRead, KinesisIO.Subscribe read) {
    this.shardId = shardId;
    this.read = read;
    this.kinesisClient =
        Suppliers.memoize((Supplier<KinesisAsyncClient> & Serializable) () ->
            read.getKinesisAsyncClientFn().apply(null));

    this.messagesToRead = messagesToRead;
  }

  @Override
  public List<Kinesis2UnboundedSource> split(int desiredNumSplits, PipelineOptions options) throws Exception {
    List<Kinesis2UnboundedSource> sources = new ArrayList<>();

    ListShardsRequest request =
        ListShardsRequest.builder().streamName(read.getStreamName()).build();
    ListShardsResponse response = kinesisClient.get().listShards(request).join();

    response.shards().forEach(s -> {

      Supplier<Queue<Record>> msgQueue = Suppliers.memoize((Supplier<Queue<Record>> & Serializable) () -> new ArrayDeque<>());
      sources.add(new Kinesis2UnboundedSource(s.shardId(), msgQueue, read));
      subscribeToShard(s.shardId(), msgQueue);
    });

    return sources;
  }

  @CanIgnoreReturnValue
  private CompletableFuture<Void> responseHandlerBuilder_VisitorBuilder(
      SubscribeToShardRequest request, Supplier<Queue<Record>> queue) {
    SubscribeToShardResponseHandler.Visitor visitor =
        SubscribeToShardResponseHandler.Visitor.builder()
            .onSubscribeToShardEvent(
                e -> e.records().forEach(r -> {
                  queue.get().add(r);
                })
            )
            .build();

    SubscribeToShardResponseHandler responseHandler =
        SubscribeToShardResponseHandler.builder()
            .onError(t -> System.err.println("Error during subscribing - " + t.getMessage()))
            .subscriber(visitor)
            .build();

    return kinesisClient.get().subscribeToShard(request, responseHandler);
  }

  private void subscribeToShard(String shardId, Supplier<Queue<Record>> queue) {
    SubscribeToShardRequest request =
        SubscribeToShardRequest.builder()
            .consumerARN(read.getConsumerArn())
            .shardId(shardId)
            .startingPosition(s -> s.type(read.getStartingPosition()))
            .build();

    responseHandlerBuilder_VisitorBuilder(request, queue);
  }

  @Override
  public UnboundedReader<Record> createReader(PipelineOptions options, @Nullable Kinesis2CheckpointMark checkpointMark) throws IOException {
    return new Kinesis2UnboundedReader(this, checkpointMark);
  }

  @Override
  public Coder<Kinesis2CheckpointMark> getCheckpointMarkCoder() {
    return SerializableCoder.of(Kinesis2CheckpointMark.class);
  }

  @Override
  public Coder<Record> getOutputCoder() {
    return SerializableCoder.of(Record.class);
  }

  @Override
  public boolean requiresDeduping() {
    return true;
  }

  public String getShardId() { return shardId; }
  public Supplier<Queue<Record>> getMessagesToRead() { return messagesToRead; }
}
