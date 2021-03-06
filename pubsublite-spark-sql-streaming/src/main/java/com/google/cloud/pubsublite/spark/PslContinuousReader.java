package com.google.cloud.pubsublite.spark;

import com.google.cloud.pubsublite.Partition;
import com.google.cloud.pubsublite.SubscriptionPath;
import com.google.cloud.pubsublite.internal.wire.PubsubContext;
import com.google.cloud.pubsublite.internal.wire.SubscriberBuilder;
import com.google.cloud.pubsublite.internal.wire.SubscriberFactory;
import com.google.cloud.pubsublite.proto.GetSubscriptionRequest;
import com.google.cloud.pubsublite.proto.PartitionCursor;
import com.google.cloud.pubsublite.proto.Subscription;
import com.google.cloud.pubsublite.proto.TopicPartitions;
import com.google.cloud.pubsublite.v1.AdminServiceClient;
import com.google.cloud.pubsublite.v1.CursorServiceClient;
import com.google.cloud.pubsublite.v1.SubscriberServiceClient;
import com.google.cloud.pubsublite.v1.SubscriberServiceSettings;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.v2.reader.InputPartition;
import org.apache.spark.sql.sources.v2.reader.streaming.ContinuousReader;
import org.apache.spark.sql.sources.v2.reader.streaming.Offset;
import org.apache.spark.sql.sources.v2.reader.streaming.PartitionOffset;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

import static com.google.cloud.pubsublite.internal.ServiceClients.addDefaultSettings;

public class PslContinuousReader implements ContinuousReader, Serializable {

    private final PslDataSourceOptions options;
    private final SubscriptionPath subscriptionPath;
    private final AdminServiceClient adminServiceClient;
    private final CursorServiceClient cursorServiceClient;
    private final MultiPartitionCommitter committer;
    private PslSourceOffset startOffset;

    public PslContinuousReader(PslDataSourceOptions options) {
        this(options,
                PslSparkUtils.getAdminClientFactory().newClient(options),
                PslSparkUtils.getCursorClientFactory().newClient(options),
                new MultiPartitionCommitter(options));
    }

    @VisibleForTesting
    public PslContinuousReader(PslDataSourceOptions options,
                               AdminServiceClient adminServiceClient,
                               CursorServiceClient cursorServiceClient,
                               MultiPartitionCommitter committer) {
        this.options = options;
        this.subscriptionPath = options.subscriptionPath();
        this.adminServiceClient = adminServiceClient;
        this.cursorServiceClient = cursorServiceClient;
        this.committer = committer;
    }

    @Override
    public Offset mergeOffsets(PartitionOffset[] offsets) {
        assert PslPartitionOffset.class.isAssignableFrom(offsets.getClass().getComponentType())
                : "PartitionOffset object is not assignable to PslPartitionOffsets.";
        return PslSourceOffset.merge(Arrays.copyOf(offsets, offsets.length, PslPartitionOffset[].class));
    }

    @Override
    public Offset deserializeOffset(String json) {
        return PslSourceOffset.fromJson(json);
    }

    @Override
    public Offset getStartOffset() {
        return startOffset;
    }

    @Override
    public void setStartOffset(Optional<Offset> start) {
        if (start.isPresent()) {
            assert PslSourceOffset.class.isAssignableFrom(start.get().getClass())
                    : "start offset is not assignable to PslSourceOffset.";
            startOffset = (PslSourceOffset) start.get();
            return;
        }

        // TODO(jiangmichael): respects consumeFromHead so it could start from head instead
        //  of always cursor.
        Subscription sub = adminServiceClient.getSubscription(subscriptionPath.toString());
        TopicPartitions topicPartitions = adminServiceClient.getTopicPartitions(sub.getTopic());

        Map<Partition, com.google.cloud.pubsublite.Offset> map = new HashMap<>();
        for (int i = 0; i < topicPartitions.getPartitionCount(); i++) {
            map.put(Partition.of(i), com.google.cloud.pubsublite.Offset.of(-1));
        }
        CursorServiceClient.ListPartitionCursorsPagedResponse resp =
                cursorServiceClient.listPartitionCursors(subscriptionPath.toString());
        for (PartitionCursor p : resp.iterateAll()) {
            map.replace(
                    Partition.of(p.getPartition()),
                    // Note that startOffset in Spark is the latest committed while offset/cursor
                    // in PSL is next to-be-delivered.
                    com.google.cloud.pubsublite.Offset.of(p.getCursor().getOffset() - 1));
        }
        startOffset = new PslSourceOffset(map);
    }

    @Override
    public void commit(Offset end) {
        assert PslSourceOffset.class.isAssignableFrom(end.getClass())
                : "end offset is not assignable to PslSourceOffset.";
        committer.commit(PslSparkUtils.addOne((PslSourceOffset) end));
    }

    @Override
    public void stop() {
        cursorServiceClient.shutdown();
        committer.close();
    }

    @Override
    public StructType readSchema() {
        return Constants.DEFAULT_SCHEMA;
    }

    @Override
    public List<InputPartition<InternalRow>> planInputPartitions() {
        return startOffset.getPartitionOffsetMap().entrySet().stream()
                .map(e ->
                    new PslContinuousInputPartition(
                            PslPartitionOffset.builder()
                                    .subscriptionPath(subscriptionPath)
                                    .partition(e.getKey())
                                    .offset(e.getValue())
                                    .build(),
                            options))
                .collect(Collectors.toList());
    }


}
