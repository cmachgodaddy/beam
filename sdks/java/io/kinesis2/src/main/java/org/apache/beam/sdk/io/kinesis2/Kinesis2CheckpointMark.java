package org.apache.beam.sdk.io.kinesis2;

import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Objects;

import java.io.IOException;
import java.io.Serializable;
import java.util.Optional;

public class Kinesis2CheckpointMark implements UnboundedSource.CheckpointMark, Serializable {

  private final transient Optional<Kinesis2UnboundedReader> reader;

  public Kinesis2CheckpointMark(Kinesis2UnboundedReader reader) {
    this.reader = Optional.of(reader);
  }

  @Override
  public void finalizeCheckpoint() throws IOException {
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Kinesis2CheckpointMark that = (Kinesis2CheckpointMark) o;
    return Objects.equal(reader, that.reader);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(reader);
  }
}
