package com.google.cloud.pubsublite.spark;

import com.google.api.core.ApiFuture;
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.pubsublite.Offset;
import com.google.cloud.pubsublite.Partition;
import com.google.cloud.pubsublite.SequencedMessage;
import com.google.cloud.pubsublite.SubscriptionPath;
import com.google.cloud.pubsublite.internal.wire.PubsubContext;
import com.google.cloud.pubsublite.internal.wire.Subscriber;
import com.google.cloud.pubsublite.internal.wire.SubscriberBuilder;
import com.google.cloud.pubsublite.proto.Cursor;
import com.google.cloud.pubsublite.proto.SeekRequest;
import com.google.cloud.pubsublite.v1.SubscriberServiceClient;
import com.google.cloud.pubsublite.v1.SubscriberServiceSettings;

import java.io.FileInputStream;

import static com.google.cloud.pubsublite.internal.ServiceClients.addDefaultSettings;

public class TestMain {

  public static void main(String[] args) throws Exception {
    String subscriptionPath =
        "projects/358307816737/locations/us-central1-a/subscriptions/test-spark-subscription";
    String credFile = "/Users/jiangmichael/key.json";

    GoogleCredentials sac =
        ServiceAccountCredentials.fromStream(new FileInputStream(credFile))
            .createScoped("https://www.googleapis.com/auth/cloud-platform");
    AccessToken accessToken = sac.refreshAccessToken();

    PslPartitionOffset startOffset =
        PslPartitionOffset.builder()
            .subscriptionPath(SubscriptionPath.parse(subscriptionPath))
            .partition(Partition.of(0))
            .offset(Offset.of(1))
            .build();
    Subscriber subscriber =
        SubscriberBuilder.newBuilder()
            .setSubscriptionPath(startOffset.subscriptionPath())
            .setPartition(startOffset.partition())
            .setContext(PubsubContext.of(Constants.FRAMEWORK))
            .setMessageConsumer(
                (msgs) -> {
                  for (SequencedMessage msg : msgs) {
                    System.out.println(msg.toString());
                  }
                })
            .build();
    subscriber.startAsync().awaitRunning();
    ApiFuture<Offset> future = subscriber
        .seek(SeekRequest.newBuilder().setCursor(Cursor.newBuilder().setOffset(1).build()).build());
    future.get();
    System.out.println("ooo");
  }
}

//    CursorServiceClient cursorServiceClient =
//        CursorServiceClient.create(
//            addDefaultSettings(
//                CloudRegion.of("us-central1"),
//                CursorServiceSettings.newBuilder()
//                    .setCredentialsProvider(
//                        () -> GoogleCredentials.create(accessToken))));
////                        () -> GoogleCredentials.fromStream(new FileInputStream(credFile)))));
////                        () -> GoogleCredentials.fromStream(new
// ByteArrayInputStream(Base64.decodeBase64(credKey))))));
////                            () -> sac)));
//
//
//    CommitCursorRequest req = CommitCursorRequest.newBuilder()
//            .setSubscription(subscriptionPath)
//            .setPartition(0)
//            .setCursor(Cursor.newBuilder().setOffset(2).build()).build();
//    CommitCursorResponse resp2 = cursorServiceClient.commitCursor(req);
//
//    CursorServiceClient.ListPartitionCursorsPagedResponse resp =
//            cursorServiceClient.listPartitionCursors(subscriptionPath);
//    List<PartitionCursor> resources = Lists.newArrayList(resp.iterateAll());
//    System.out.println(resources.size());
//
//    for (PartitionCursor p : resp.iterateAll()) {
//      System.out.println(p.getPartition());
//      System.out.println(p.getCursor().getOffset());
//    }
