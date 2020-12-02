package com.google.cloud.pubsublite.spark;

import com.google.cloud.pubsublite.*;
import com.google.cloud.pubsublite.internal.PullSubscriber;
import com.google.protobuf.ByteString;
import com.google.protobuf.util.Timestamps;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader;
import org.junit.Test;

import java.util.Optional;

import static com.google.common.truth.Truth.assertThat;
import static org.mockito.Mockito.*;

public class PslContinuousInputPartitionTest {

    private static SubscriptionPath TEST_SUB = SubscriptionPath.newBuilder().setProject(ProjectNumber.of(123456))
            .setLocation(CloudZone.of(CloudRegion.of("us-central1"), 'a'))
            .setName(SubscriptionName.of("test-sub")).build();
    private static Partition TEST_PARTITION = Partition.of(3);
    @SuppressWarnings("unchecked")
    private PullSubscriber<SequencedMessage> subscriber = (PullSubscriber<SequencedMessage>) mock(PullSubscriber.class);
    private InputPartitionReader<InternalRow> reader;

    private static SequencedMessage newMessage(long offset) {
        return SequencedMessage.of(
                Message.builder()
                        .setData(ByteString.copyFromUtf8("text"))
                        .build(),
                Timestamps.EPOCH,
                Offset.of(offset),
                10000);
    }

    private void createReader(long startOffset) {
        reader = PslContinuousInputPartition.createPartitionReader(
                PslPartitionOffset.builder().subscriptionPath(TEST_SUB)
                        .partition(TEST_PARTITION)
                        .offset(Offset.of(startOffset)).build(), subscriber);
    }

    @Test
    public void testPartitionReader() throws Exception {
        createReader(5);
        SequencedMessage message1 = newMessage(10);
        InternalRow expectedRow1 = PslSparkUtils.toInternalRow(message1, TEST_SUB, TEST_PARTITION);
        SequencedMessage message2 = newMessage(13);
        InternalRow expectedRow2 = PslSparkUtils.toInternalRow(message2, TEST_SUB, TEST_PARTITION);

        // Multiple get w/o next will return same msg, and only pull once.
        when(subscriber.pullOne()).thenReturn(Optional.of(message1));
        InternalRow row = reader.get();
        assertThat(row).isEqualTo(expectedRow1);
        row = reader.get();
        assertThat(row).isEqualTo(expectedRow1);
        verify(subscriber, times(1)).pullOne();

        // Next will advance to next msg.
        when(subscriber.hasNext()).thenReturn(true);
        assertThat(reader.next()).isTrue();
        when(subscriber.pullOne()).thenReturn(Optional.of(message2));
        row = reader.get();
        assertThat(row).isEqualTo(expectedRow2);
        verify(subscriber, times(2)).pullOne();
    }

}