package com.google.cloud.pubsublite.spark;

import com.google.cloud.pubsublite.*;
import com.google.cloud.pubsublite.proto.Cursor;
import com.google.cloud.pubsublite.proto.PartitionCursor;
import com.google.cloud.pubsublite.v1.CursorServiceClient;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.Test;

import java.util.Optional;

import static com.google.common.truth.Truth.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

public class PslContinuousReaderTest {

    private static final SubscriptionName TEST_SUB_NAME = SubscriptionName.of("test-sub");
    private static final SubscriptionPath TEST_SUB = SubscriptionPath.newBuilder().setProject(ProjectNumber.of(123456))
            .setLocation(CloudZone.of(CloudRegion.of("us-central1"), 'a'))
            .setName(TEST_SUB_NAME).build();
    private static final long TEST_OFFSET = 10L;
    private static final PslDataSourceOptions OPTIONS = PslDataSourceOptions.builder()
            .subscriptionPath(TEST_SUB)
            .credentialsAccessToken("abc")
            .build();
    private final CursorServiceClient cursorClient = mock(CursorServiceClient.class);
    private final MultiPartitionCommitter committer = mock(MultiPartitionCommitter.class);

//    @Test
//    public void testEmptyStartOffset() {
//        PslContinuousReader reader = new PslContinuousReader(OPTIONS, cursorClient, committer);
//
//        CursorServiceClient.ListPartitionCursorsPagedResponse resp =
//                mock(CursorServiceClient.ListPartitionCursorsPagedResponse.class);
//        when(resp.iterateAll()).thenReturn(ImmutableList.of(
//                PartitionCursor.newBuilder()
//                        .setPartition(1)
//                        .setCursor(Cursor.newBuilder().setOffset(TEST_OFFSET).build())
//                        .build()
//        ));
//        doReturn(resp).when(cursorClient).listPartitionCursors(anyString());
//
//        reader.setStartOffset(Optional.empty());
//        assertThat(((PslSourceOffset)reader.getStartOffset()).getPartitionOffsetMap())
//                .containsExactly(Partition.of(1), Offset.of(TEST_OFFSET - 1));
//    }
//
//    @Test
//    public void testValidStartOffset() {
//        PslContinuousReader reader = new PslContinuousReader(OPTIONS, cursorClient, committer);
//
//        PslSourceOffset offset = new PslSourceOffset(ImmutableMap.of(Partition.of(1), Offset.of(TEST_OFFSET)));
//        reader.setStartOffset(Optional.of(offset));
//        assertThat(reader.getStartOffset()).isEqualTo(offset);
//    }
//
//    @Test
//    public void testMergeOffsets() {
//        PslContinuousReader reader = new PslContinuousReader(OPTIONS, cursorClient, committer);
//
//        PslPartitionOffset po1 = PslPartitionOffset.builder()
//                .subscriptionPath(TEST_SUB)
//                .partition(Partition.of(1))
//                .offset(Offset.of(10)).build();
//        PslPartitionOffset po2 = PslPartitionOffset.builder()
//                .subscriptionPath(TEST_SUB)
//                .partition(Partition.of(2))
//                .offset(Offset.of(5)).build();
//        assertThat(reader.mergeOffsets(new PslPartitionOffset[]{po1, po2}))
//                .isEqualTo(PslSourceOffset.merge(new PslPartitionOffset[]{po1, po2}));
//    }
//
//    @Test
//    public void testDeserializeOffset() {
//        PslContinuousReader reader = new PslContinuousReader(OPTIONS, cursorClient, committer);
//
//        PslSourceOffset offset = new PslSourceOffset(ImmutableMap.of(Partition.of(1), Offset.of(TEST_OFFSET)));
//        assertThat(reader.deserializeOffset(offset.json())).isEqualTo(offset);
//    }
//
//    @Test
//    public void testCommit() {
//        PslContinuousReader reader = new PslContinuousReader(OPTIONS, cursorClient, committer);
//
//        PslSourceOffset offset = new PslSourceOffset(ImmutableMap.of(
//                Partition.of(1), Offset.of(10L),
//                Partition.of(2), Offset.of(8L)
//        ));
//        PslSourceOffset expectedCommitOffset = new PslSourceOffset(ImmutableMap.of(
//                Partition.of(1), Offset.of(11L),
//                Partition.of(2), Offset.of(9L)
//        ));
//        reader.commit(offset);
//        verify(committer, times(1)).commit(eq(expectedCommitOffset));
//    }
}