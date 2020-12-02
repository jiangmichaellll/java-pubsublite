package com.google.cloud.pubsublite.spark;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.cloud.pubsublite.*;
import com.google.cloud.pubsublite.cloudpubsub.Publisher;
import com.google.cloud.pubsublite.cloudpubsub.PublisherSettings;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class Main {

    public static void main(String[] args) throws Exception {

        String accessToken =
                "ya29.c.Kp0B5weD2SwVlitl6g5GgvJbBNcwaL_bwom7h_E-qSvFUqX_0_7hdFPwAwJhuvVeDVbAXGcJQVztD7Z5SHLnN6seScwQc8NyG3mPzoHW8t3ReFboRa5yBJJcf9MPTzGgw27HRsGiohAv-9jO876fEP2EW68jkbVRYBd140QbrzGxUjlFyYhZPCgygfkwtapWuDhDAF774-kCaA5cYilfYw";

//        // Publish some messages.
//        PublisherServiceClient publisherServiceClient = PublisherServiceClient.create(addDefaultSettings(
//                CloudRegion.of("us-central1"),
//                PublisherServiceSettings.newBuilder()
//                        .setCredentialsProvider(() ->
//                                GoogleCredentials.create(new AccessToken(accessToken, null))
//                        )
//        ));
//        SinglePartitionPublisherBuilder.Builder publisherBuilder = SinglePartitionPublisherBuilder.newBuilder()
//                .setBatchingSettings(BatchingSettings.newBuilder()
//                        .setIsEnabled(false)
//                        .build())
//                .setContext(PubsubContext.of(Constants.FRAMEWORK))
//                .setTopic(TopicPath.newBuilder()
//                        .setProject(ProjectNumber.of(358307816737L))
//                        .setLocation(CloudZone.of(CloudRegion.of("us-central1"), 'a'))
//                        .setName(TopicName.of("test-spark-jiangmichael"))
//                        .build())
//                .setPartition(Partition.of(1))
//                .setServiceClient(publisherServiceClient);
//        Publisher<PublishMetadata> publisher = publisherBuilder.build();
//        System.out.println("Publisher starting");
//        publisher.startAsync();
//        publisher.awaitRunning();
//        System.out.println("Publisher running");
//        for (int i = 0; i < 100; i++) {
//            Instant now = Instant.now();
//            Message message = Message.builder()
//                    .setData(ByteString.copyFromUtf8("data" + i))
//                    .setEventTime(Timestamp.newBuilder()
//                            .setSeconds(now.getEpochSecond())
//                            .setNanos(now.getNano()).build())
//                    .build();
//            ApiFuture<PublishMetadata> future = publisher.publish(message);
//            ApiFutures.addCallback(future, new ApiFutureCallback<PublishMetadata>() {
//                @Override
//                public void onFailure(Throwable throwable) {
//                    System.out.println("failed to publish: " + throwable);
//                }
//
//                @Override
//                public void onSuccess(PublishMetadata s) {
//                    System.out.println("published message : " + s.encode());
//                }
//            }, MoreExecutors.directExecutor());
//        }

//
//        long projectNumber = 358307816737L;
//        String cloudRegion = "us-central1";
//        char zoneId = 'a';
//        String topicId = "test-spark-jiangmichael";
//        int messageCount = 100;
//
//        TopicPath topicPath =
//                TopicPath.newBuilder()
//                        .setProject(ProjectNumber.of(projectNumber))
//                        .setLocation(CloudZone.of(CloudRegion.of(cloudRegion), zoneId))
//                        .setName(TopicName.of(topicId))
//                        .build();
//        Publisher publisher = null;
//        List<ApiFuture<String>> futures = new ArrayList<>();
//
//        try {
//            PublisherSettings publisherSettings =
//                    PublisherSettings.newBuilder().setTopicPath(topicPath).build();
//
//            publisher = Publisher.create(publisherSettings);
//
//            // Start the publisher. Upon successful starting, its state will become RUNNING.
//            publisher.startAsync().awaitRunning();
//
//            for (int i = 0; i < messageCount; i++) {
//                String message = "message-" + i;
//
//                // Convert the message to a byte string.
//                ByteString data = ByteString.copyFromUtf8(message);
//                PubsubMessage pubsubMessage = PubsubMessage.newBuilder().setData(data).build();
//
//                // Publish a message. Messages are automatically batched.
//                ApiFuture<String> future = publisher.publish(pubsubMessage);
//                futures.add(future);
//            }
//        } finally {
//            ArrayList<PublishMetadata> metadata = new ArrayList<>();
//            List<String> ackIds = ApiFutures.allAsList(futures).get();
//            for (String id : ackIds) {
//                // Decoded metadata contains partition and offset.
//                metadata.add(PublishMetadata.decode(id));
//            }
//            System.out.println(metadata + "\nPublished " + ackIds.size() + " messages.");
//
//            if (publisher != null) {
//                // Shut down the publisher.
//                publisher.stopAsync().awaitTerminated();
//                System.out.println("Publisher is shut down.");
//            }
//        }


        SparkSession spark = SparkSession.builder()
                .appName("testapp").config("spark.master",
                        "spark://jiangmichael-macbookpro2.roam.corp.google.com:7077")
                .getOrCreate();
        try {
            spark.readStream().format("pubsublite")
                    .option(Constants.CREDENTIALS_FILE_CONFIG_KEY, "/Users/jiangmichael/key.json")
                    .option("pubsublite.subscription",
                            "projects/358307816737/locations/us-central1-a/subscriptions/test-spark-subscription")
                    .load()
                    .writeStream()
                    .format("console")
                    .outputMode(OutputMode.Append())
                    .trigger(Trigger.Continuous(1, TimeUnit.SECONDS))
                    .start().awaitTermination();
        } catch (StreamingQueryException e) {
            e.printStackTrace();
        }


    }
}
