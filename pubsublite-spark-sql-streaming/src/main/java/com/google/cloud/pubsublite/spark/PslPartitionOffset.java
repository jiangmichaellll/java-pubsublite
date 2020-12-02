package com.google.cloud.pubsublite.spark;

import com.google.auto.value.AutoValue;
import com.google.cloud.pubsublite.Offset;
import com.google.cloud.pubsublite.Partition;
import com.google.cloud.pubsublite.SubscriptionPath;
import org.apache.spark.sql.sources.v2.reader.streaming.PartitionOffset;

import java.io.Serializable;

@AutoValue
public abstract class PslPartitionOffset implements PartitionOffset, Serializable {
  private static final long serialVersionUID = -3398208694782540866L;

  public abstract SubscriptionPath subscriptionPath();

  public abstract Partition partition();

  public abstract Offset offset();

  public static Builder builder() {
    return new AutoValue_PslPartitionOffset.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder subscriptionPath(SubscriptionPath subscriptionPath);

    public abstract Builder partition(Partition partition);

    public abstract Builder offset(Offset offset);

    public abstract PslPartitionOffset build();
  }
}
