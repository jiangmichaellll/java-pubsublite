package com.google.cloud.pubsublite.spark;

import com.google.api.gax.rpc.ApiException;
import com.google.cloud.pubsublite.Message;
import com.google.cloud.pubsublite.Offset;
import com.google.cloud.pubsublite.SequencedMessage;
import com.google.cloud.pubsublite.cloudpubsub.FlowControlSettings;
import com.google.cloud.pubsublite.internal.BufferingPullSubscriber;
import com.google.cloud.pubsublite.internal.CheckedApiException;
import com.google.cloud.pubsublite.internal.PullSubscriber;
import com.google.cloud.pubsublite.internal.wire.PubsubContext;
import com.google.cloud.pubsublite.internal.wire.Subscriber;
import com.google.cloud.pubsublite.internal.wire.SubscriberBuilder;
import com.google.cloud.pubsublite.proto.Cursor;
import com.google.cloud.pubsublite.proto.PubSubMessage;
import com.google.cloud.pubsublite.proto.SeekRequest;
import com.google.cloud.pubsublite.v1.SubscriberServiceClient;
import com.google.cloud.pubsublite.v1.SubscriberServiceSettings;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.flogger.GoogleLogger;
import com.google.protobuf.Timestamp;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.v2.reader.ContinuousInputPartition;
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader;
import org.apache.spark.sql.sources.v2.reader.streaming.ContinuousInputPartitionReader;
import org.apache.spark.sql.sources.v2.reader.streaming.PartitionOffset;

import java.io.IOException;
import java.io.Serializable;
import java.util.Optional;
import java.util.function.Consumer;

import static com.google.cloud.pubsublite.internal.ServiceClients.addDefaultSettings;

public class PslContinuousInputPartition implements ContinuousInputPartition<InternalRow>, Serializable {
    public interface SubscriberFactory extends Serializable {
        Subscriber newSubscriber(Consumer<ImmutableList<SequencedMessage>> consumer) throws ApiException;
    }

    private static final GoogleLogger log = GoogleLogger.forEnclosingClass();

    private final PslPartitionOffset startOffset;
    private final PslDataSourceOptions options;
    private final SubscriberFactory factory;

    public PslContinuousInputPartition(PslPartitionOffset startOffset, PslDataSourceOptions options) {
        this.startOffset = startOffset;
        this.options = options;
        this.factory = (consumer) -> {
            return SubscriberBuilder.newBuilder()
                    .setSubscriptionPath(startOffset.subscriptionPath())
                    .setPartition(startOffset.partition())
                    .setContext(PubsubContext.of(Constants.FRAMEWORK))
//                        .setServiceClient(SubscriberServiceClient.create(
//                                addDefaultSettings(
//                                        startOffset.subscriptionPath().location().region(),
//                                        SubscriberServiceSettings.newBuilder()
//                                                .setCredentialsProvider(
//
//                                )))
                    .setMessageConsumer(consumer)
                    .build();
        };
    }

    @VisibleForTesting
    public static InputPartitionReader<InternalRow> createPartitionReader(PslPartitionOffset currentOffset,
                                                                          PullSubscriber<SequencedMessage> subscriber) {
        return new PslContinuousInputPartitionReader(currentOffset, subscriber);
    }

    @Override
    public InputPartitionReader<InternalRow> createContinuousReader(PartitionOffset offset) {
        assert PslPartitionOffset.class.isAssignableFrom(offset.getClass())
                : "offset is not assignable to PslPartitionOffset";

        PslPartitionOffset pslOffset = (PslPartitionOffset) offset;
        PslPartitionOffset currentOffset = PslPartitionOffset.builder()
                .subscriptionPath(pslOffset.subscriptionPath())
                .partition(pslOffset.partition())
                // The first message to read is startOffset + 1
                .offset(Offset.of(pslOffset.offset().value() + 1))
                .build();

        BufferingPullSubscriber subscriber;
        try {
            subscriber =
                    new BufferingPullSubscriber(
                            factory::newSubscriber,
                            FlowControlSettings.builder()
                                    .setBytesOutstanding(options.maxBytesOutstanding())
                                    .setMessagesOutstanding(options.maxMessagesOutstanding())
                                    .build(),
                            SeekRequest.newBuilder()
                                    .setCursor(
                                            Cursor.newBuilder().setOffset(currentOffset.offset().value()).build())
                                    .build());
        } catch (CheckedApiException e) {
            throw new IllegalStateException(
                    "Unable to create PSL subscriber for " + startOffset.toString(), e);
        }
        return createPartitionReader((PslPartitionOffset) offset, subscriber);
    }

    @Override
    public InputPartitionReader<InternalRow> createPartitionReader() {
        return createContinuousReader(startOffset);
    }

    private static class PslContinuousInputPartitionReader
            implements ContinuousInputPartitionReader<InternalRow> {

        private final PullSubscriber<SequencedMessage> subscriber;
        private PslPartitionOffset currentOffset;
        private Optional<SequencedMessage> currentMsg = Optional.empty();

        private PslContinuousInputPartitionReader(
                PslPartitionOffset currentOffset, PullSubscriber<SequencedMessage> subscriber) {
            this.currentOffset = currentOffset;
            this.subscriber = subscriber;
            log.atWarning().log("oooooooo");
        }

        @Override
        public PartitionOffset getOffset() {
            return currentOffset;
        }

        @Override
        public boolean next() {
            currentMsg = Optional.empty();
            return true;
//            try {
//                while (!subscriber.hasNext()) {
//                    Thread.sleep(10);
//                }
//                return true;
//            } catch (CheckedApiException | InterruptedException e) {
//                throw new IllegalStateException("Failed to call subscriber hasNext.", e);
//            }
        }

        @Override
        public InternalRow get() {
            if (!currentMsg.isPresent()) {
//                try {
                    currentMsg = Optional.of(SequencedMessage.of(Message.builder().build(),
                            Timestamp.getDefaultInstance(), Offset.of(10), 10L));
//                    currentMsg = subscriber.pullOne();
                    assert currentMsg.isPresent()
                            : "Unable to pull message from subscriber for " + currentOffset.toString();
                    currentOffset =
                            PslPartitionOffset.builder()
                                    .subscriptionPath(currentOffset.subscriptionPath())
                                    .partition(currentOffset.partition())
                                    .offset(currentMsg.get().offset())
                                    .build();

//                } catch (CheckedApiException e) {
//                    throw new IllegalStateException(
//                            "Unable to pull message from subscriber for " + currentOffset.toString(), e);
//                }
            }
            log.atWarning().log("oh message :" + currentMsg.get().toString());
            return PslSparkUtils.toInternalRow(currentMsg.get(), currentOffset.subscriptionPath(),
                    currentOffset.partition());
        }

        @Override
        public void close() {
            try {
                subscriber.close();
            } catch (Exception e) {
                log.atWarning().log("Subscriber failed to close.");
            }
        }
    }
}
