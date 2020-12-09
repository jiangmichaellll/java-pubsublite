package com.google.cloud.pubsublite.spark;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.fail;

import com.google.cloud.pubsublite.*;
import com.google.common.collect.ImmutableMap;
import org.junit.Test;

public class PslSourceOffsetTest {

  private static final SubscriptionPath TEST_SUB;

  static {
    TEST_SUB =
        SubscriptionPath.newBuilder()
            .setProject(ProjectId.of("test-project"))
            .setLocation(CloudZone.of(CloudRegion.of("us-central1"), 'a'))
            .setName(SubscriptionName.of("test-sub"))
            .build();
  }

  @Test
  public void roundTrip() {
    PslSourceOffset offset =
        new PslSourceOffset(
            ImmutableMap.of(
                Partition.of(3L), Offset.of(Long.MAX_VALUE - 1000),
                Partition.of(1L), Offset.of(5L),
                Partition.of(2L), Offset.of(8L)));
    assertThat(offset.json()).isEqualTo("{\"1\":5,\"2\":8,\"3\":9223372036854774807}");
    assertThat(PslSourceOffset.fromJson(offset.json())).isEqualTo(offset);
  }

  @Test
  public void mergePslSourceOffsets() {
    PslSourceOffset o1 =
        new PslSourceOffset(
            ImmutableMap.of(
                Partition.of(3L), Offset.of(10L),
                Partition.of(1L), Offset.of(5L),
                Partition.of(2L), Offset.of(8L)));
    PslSourceOffset o2 =
        new PslSourceOffset(
            ImmutableMap.of(
                Partition.of(3L), Offset.of(8L),
                Partition.of(2L), Offset.of(11L),
                Partition.of(4L), Offset.of(1L)));
    PslSourceOffset expected =
        new PslSourceOffset(
            ImmutableMap.of(
                Partition.of(1L), Offset.of(5L),
                Partition.of(2L), Offset.of(11L),
                Partition.of(3L), Offset.of(10L),
                Partition.of(4L), Offset.of(1L)));
    assertThat(PslSourceOffset.merge(o1, o2)).isEqualTo(expected);
  }

  @Test
  public void mergePslPartitionOffsetsDuplicatePartition() {
    PslPartitionOffset[] offsets = {
      PslPartitionOffset.builder()
          .subscriptionPath(TEST_SUB)
          .partition(Partition.of(3L))
          .offset(Offset.of(10L))
          .build(),
      PslPartitionOffset.builder()
          .subscriptionPath(TEST_SUB)
          .partition(Partition.of(1L))
          .offset(Offset.of(5L))
          .build(),
      PslPartitionOffset.builder()
          .subscriptionPath(TEST_SUB)
          .partition(Partition.of(1L))
          .offset(Offset.of(4L))
          .build()
    };
    try {
      PslSourceOffset.merge(offsets);
      fail();
    } catch (AssertionError e) {
      assertThat(e).hasMessageThat().contains("same partition");
    }
  }

  @Test
  public void mergePslPartitionOffsets() {
    PslPartitionOffset[] offsets = {
      PslPartitionOffset.builder()
          .subscriptionPath(TEST_SUB)
          .partition(Partition.of(3L))
          .offset(Offset.of(10L))
          .build(),
      PslPartitionOffset.builder()
          .subscriptionPath(TEST_SUB)
          .partition(Partition.of(1L))
          .offset(Offset.of(5L))
          .build(),
      PslPartitionOffset.builder()
          .subscriptionPath(TEST_SUB)
          .partition(Partition.of(2L))
          .offset(Offset.of(4L))
          .build()
    };
    PslSourceOffset expected =
        new PslSourceOffset(
            ImmutableMap.of(
                Partition.of(3L), Offset.of(10L),
                Partition.of(1L), Offset.of(5L),
                Partition.of(2L), Offset.of(4L)));
    assertThat(PslSourceOffset.merge(offsets)).isEqualTo(expected);
  }
}
