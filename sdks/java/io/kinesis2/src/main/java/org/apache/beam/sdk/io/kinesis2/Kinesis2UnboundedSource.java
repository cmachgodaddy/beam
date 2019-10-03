package org.apache.beam.sdk.io.kinesis2;

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

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;

public class Kinesis2UnboundedSource extends UnboundedSource<Record, Kinesis2CheckpointMark> {

  private final String shardId;
  private final KinesisIO.Subscribe read;
  private final Supplier<KinesisAsyncClient> kinesisClient;

  public Kinesis2UnboundedSource(String shardId, KinesisIO.Subscribe read) {
    this.shardId = shardId;
    this.read = read;
    this.kinesisClient =
        Suppliers.memoize((Supplier<KinesisAsyncClient> & Serializable) () ->
            read.getKinesisAsyncClientFn().apply(null));
  }

  @Override
  public List<Kinesis2UnboundedSource> split(int desiredNumSplits, PipelineOptions options) throws Exception {
    List<Kinesis2UnboundedSource> sources = new ArrayList<>();

    ListShardsRequest request =
        ListShardsRequest.builder().streamName(read.getStreamName()).build();
    ListShardsResponse response = kinesisClient.get().listShards(request).join();

    response.shards().forEach(s ->
        sources.add(new Kinesis2UnboundedSource(s.shardId(), read))
    );

    return sources;
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

  public KinesisIO.Subscribe getReader() { return read; }
  public KinesisAsyncClient getKinesisClient() { return kinesisClient.get(); }
  public String getShardId() { return shardId; }
}
