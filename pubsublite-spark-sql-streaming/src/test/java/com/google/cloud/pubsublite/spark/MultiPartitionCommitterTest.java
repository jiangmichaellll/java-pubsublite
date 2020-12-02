package com.google.cloud.pubsublite.spark;

import com.google.api.core.SettableApiFuture;
import com.google.cloud.pubsublite.*;
import com.google.cloud.pubsublite.internal.wire.Committer;
import com.google.common.collect.ImmutableMap;
import org.junit.Test;

import java.util.Optional;
import java.util.concurrent.ExecutionException;

import static com.google.common.truth.Truth.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

public class MultiPartitionCommitterTest {

    private static final SubscriptionName TEST_SUB_NAME = SubscriptionName.of("test-sub");
    private static final SubscriptionPath TEST_SUB = SubscriptionPath.newBuilder().setProject(ProjectNumber.of(123456))
            .setLocation(CloudZone.of(CloudRegion.of("us-central1"), 'a'))
            .setName(TEST_SUB_NAME).build();
    private static final PslDataSourceOptions OPTIONS = PslDataSourceOptions.builder()
            .subscriptionPath(TEST_SUB)
            .credentialsAccessToken("abc")
            .build();

    @Test
    public void testCommit() {
        Committer committer1 = mock(Committer.class);
        Committer committer2 = mock(Committer.class);
        MultiPartitionCommitter multiCommitter = new MultiPartitionCommitter(OPTIONS, (o, p) -> {
            if (p.value() == 1L) {
                return committer1;
            } else {
                return committer2;
            }
        });

        PslSourceOffset offset = new PslSourceOffset(ImmutableMap.of(
                Partition.of(1), Offset.of(10L),
                Partition.of(2), Offset.of(8L)
        ));
        SettableApiFuture<Void> future1 = SettableApiFuture.create();
        SettableApiFuture<Void> future2 = SettableApiFuture.create();
        when(committer1.commitOffset(eq(Offset.of(10L)))).thenReturn(future1);
        when(committer2.commitOffset(eq(Offset.of(8L)))).thenReturn(future2);
        multiCommitter.commit(offset);
        future1.set(null);

        offset = new PslSourceOffset(ImmutableMap.of(
                Partition.of(1), Offset.of(12L),
                Partition.of(2), Offset.of(13L)
        ));
        SettableApiFuture<Void> future3 = SettableApiFuture.create();
        SettableApiFuture<Void> future4 = SettableApiFuture.create();
        when(committer1.commitOffset(eq(Offset.of(12L)))).thenReturn(future3);
        when(committer2.commitOffset(eq(Offset.of(13L)))).thenReturn(future4);
        multiCommitter.commit(offset);
        assertThat(future2.isCancelled()).isTrue();
        future3.setException(new ExecutionException(new InternalError("failed")));
        future4.set(null);
    }

    @Test
    public void testClose() {
        Committer committer = mock(Committer.class);
        MultiPartitionCommitter multiCommitter = new MultiPartitionCommitter(OPTIONS, (o, p) -> committer);

        PslSourceOffset offset = new PslSourceOffset(ImmutableMap.of(Partition.of(1), Offset.of(10L)));
        SettableApiFuture<Void> future1 = SettableApiFuture.create();
        when(committer.commitOffset(eq(Offset.of(10L)))).thenReturn(future1);
        when(committer.stopAsync()).thenReturn(committer);
        multiCommitter.commit(offset);

        multiCommitter.close();
        verify(committer, times(1)).stopAsync();
    }
}