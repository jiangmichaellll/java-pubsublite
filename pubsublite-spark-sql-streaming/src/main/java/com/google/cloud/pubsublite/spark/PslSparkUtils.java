package com.google.cloud.pubsublite.spark;

import com.google.cloud.pubsublite.Offset;
import com.google.cloud.pubsublite.Partition;
import com.google.cloud.pubsublite.SequencedMessage;
import com.google.cloud.pubsublite.SubscriptionPath;
import com.google.cloud.pubsublite.v1.AdminServiceClient;
import com.google.cloud.pubsublite.v1.AdminServiceSettings;
import com.google.cloud.pubsublite.v1.CursorServiceClient;
import com.google.cloud.pubsublite.v1.CursorServiceSettings;
import com.google.common.collect.ImmutableList;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.unsafe.types.ByteArray;
import org.apache.spark.unsafe.types.UTF8String;
import scala.collection.JavaConverters;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.google.cloud.pubsublite.internal.ServiceClients.addDefaultSettings;

public class PslSparkUtils {
    public static InternalRow toInternalRow(SequencedMessage msg, SubscriptionPath subscription, Partition partition) {

        List<Object> list = ImmutableList.of(
                UTF8String.fromString(subscription.toString()),
                partition.value(),
                msg.offset().value(),
                ByteArray.concat(msg.message().key().toByteArray()),
                ByteArray.concat(msg.message().data().toByteArray()),
                msg.publishTime().getSeconds(),
                0L);
//                msg.message().eventTime(),
//                msg.message().attributes());
        return InternalRow.fromSeq(
                JavaConverters.asScalaBufferConverter(list).asScala().toSeq());

    }

    public static CursorClientFactory getCursorClientFactory() {
        return (options) -> {
            try {
                return CursorServiceClient.create(addDefaultSettings(
                        options.subscriptionPath().location().region(),
                        CursorServiceSettings.newBuilder().setCredentialsProvider(
                                new PslCredentialsProvider(options)))
                );
            } catch (IOException e) {
                throw new IllegalStateException("Unable to create CursorServiceClient.");
            }
        };
    }

    public static AdminClientFactory getAdminClientFactory() {
        return (options) -> {
            try {
                return AdminServiceClient.create(addDefaultSettings(
                        options.subscriptionPath().location().region(),
                        AdminServiceSettings.newBuilder().setCredentialsProvider(
                                new PslCredentialsProvider(options)))
                );
            } catch (IOException e) {
                throw new IllegalStateException("Unable to create AdminServiceClient.");
            }
        };
    }

    public static PslSourceOffset addOne(PslSourceOffset offset) {
        Map<Partition, Offset> map = new HashMap<>(offset.getPartitionOffsetMap());
        map.replaceAll((k, v) -> Offset.of(v.value() + 1));
        return new PslSourceOffset(map);
    }

    public interface CursorClientFactory extends Serializable {
        CursorServiceClient newClient(PslDataSourceOptions options);
    }

    public interface AdminClientFactory extends Serializable {
        AdminServiceClient newClient(PslDataSourceOptions options);
    }
}
