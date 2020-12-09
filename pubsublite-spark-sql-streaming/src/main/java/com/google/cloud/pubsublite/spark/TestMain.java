package com.google.cloud.pubsublite.spark;

import com.google.api.core.ApiFuture;
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.pubsublite.*;
import com.google.cloud.pubsublite.internal.wire.PubsubContext;
import com.google.cloud.pubsublite.internal.wire.Subscriber;
import com.google.cloud.pubsublite.internal.wire.SubscriberBuilder;
import com.google.cloud.pubsublite.proto.*;
import com.google.cloud.pubsublite.v1.*;
import com.google.common.collect.Lists;

import java.io.FileInputStream;
import java.util.List;

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


//    CursorServiceClient cursorServiceClient =
//        CursorServiceClient.create(
//            addDefaultSettings(
//                CloudRegion.of("us-central1"),
//                CursorServiceSettings.newBuilder()
//                    .setCredentialsProvider(
//                        () -> GoogleCredentials.create(accessToken))));
////                        () -> GoogleCredentials.fromStream(new FileInputStream(credFile)))));
////                        () -> GoogleCredentials.fromStream(new
//// ByteArrayInputStream(Base64.decodeBase64(credKey))))));
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



    PslDataSourceOptions options = PslDataSourceOptions.builder()
            .subscriptionPath(SubscriptionPath.parse(subscriptionPath))
//            .credentialsFile(credFile)
            .build();



    AdminServiceClient adminServiceClient =
        AdminServiceClient.create(
            addDefaultSettings(
                CloudRegion.of("us-central1"),
                AdminServiceSettings.newBuilder()
                    .setCredentialsProvider(
                            new PslCredentialsProvider(options))));
//                        () -> GoogleCredentials.fromStream(new FileInputStream(credFile)))));
//                        () -> GoogleCredentials.fromStream(new
// ByteArrayInputStream(Base64.decodeBase64(credKey))))));
//                            () -> sac)));
    Subscription sub = adminServiceClient.getSubscription(subscriptionPath);
    System.out.println(sub.getTopic());







  }
}






//  PslPartitionOffset startOffset =
//          PslPartitionOffset.builder()
//                  .subscriptionPath(SubscriptionPath.parse(subscriptionPath))
//                  .partition(Partition.of(0))
//                  .offset(Offset.of(1))
//                  .build();
//  Subscriber subscriber =
//          SubscriberBuilder.newBuilder()
//                  .setSubscriptionPath(startOffset.subscriptionPath())
//                  .setPartition(startOffset.partition())
//                  .setContext(PubsubContext.of(Constants.FRAMEWORK))
//                  .setMessageConsumer(
//                          (msgs) -> {
//                            for (SequencedMessage msg : msgs) {
//                              System.out.println(msg.toString());
//                            }
//                          })
//                  .build();
//    subscriber.startAsync().awaitRunning();
//            ApiFuture<Offset> future = subscriber
//        .seek(SeekRequest.newBuilder().setCursor(Cursor.newBuilder().setOffset(1).build()).build());
//        future.get();
//        System.out.println("ooo");