/*
 * Copyright 2020 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.pubsublite.kafka;

import com.google.cloud.pubsublite.*;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import io.grpc.StatusException;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.UnsupportedVersionException;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

public class FakeConsumer implements Consumer<byte[], byte[]> {
    private static Duration INFINITE_DURATION = Duration.ofMillis(Long.MAX_VALUE);
    private static long STARTING_OFFSET = 10;
    private static long ENDING_OFFSET = 1000000;
    private static TopicPartition TOPIC_PARTITION = new TopicPartition("test-topic", 0);
    private long position = STARTING_OFFSET;

    public FakeConsumer() {
    }

    @Override
    public ConsumerRecords<byte[], byte[]> poll(Duration duration) {
        try {
            Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> records = new HashMap<>();
            List<ConsumerRecord<byte[], byte[]>> rl = new ArrayList<>();
            SequencedMessage sequencedMessage =
                    SequencedMessage.of(
                            Message.builder()
                                    .setKey(ByteString.copyFromUtf8("abc"))
                                    .setData(ByteString.copyFromUtf8("def"))
                                    .setEventTime(Timestamp.newBuilder().setSeconds(1).setNanos(1000000).build())
                                    .setAttributes(
                                            ImmutableListMultimap.of(
                                                    "xxx",
                                                    ByteString.copyFromUtf8("yyy"),
                                                    "zzz",
                                                    ByteString.copyFromUtf8("zzz"),
                                                    "zzz",
                                                    ByteString.copyFromUtf8("zzz")))
                                    .build(), Timestamp.newBuilder().setNanos(12345).build(), Offset.of(this.position), 123L);
            rl.add(RecordTransforms.fromMessage(sequencedMessage, TopicPath.newBuilder()
                    .setProject(ProjectNumber.of(123))
                    .setLocation(CloudZone.of(CloudRegion.of("us-central1"), 'a'))
                    .setName(TopicName.of("test-topic"))
                    .build(), Partition.of(0)));
            records.put(TOPIC_PARTITION, rl);
            this.position++;
            return new ConsumerRecords<>(records);
        } catch (StatusException e) {
            return ConsumerRecords.empty();
        }
    }

    @Override
    public Set<TopicPartition> assignment() {
        return ImmutableSet.of(TOPIC_PARTITION);
    }

    @Override
    public Set<String> subscription() {
        return ImmutableSet.of("/projects/1234/locations/us-central1/topics/test-topic");
    }

    @Override
    public void subscribe(Pattern pattern) {
    }

    @Override
    public void subscribe(Pattern pattern, ConsumerRebalanceListener consumerRebalanceListener) {
    }

    @Override
    public void subscribe(Collection<String> collection) {
    }

    @Override
    public void subscribe(
            Collection<String> collection, ConsumerRebalanceListener consumerRebalanceListener) {
    }

    @Override
    public void assign(Collection<TopicPartition> collection) {
    }

    @Override
    public void unsubscribe() {
    }

    @Override
    public ConsumerRecords<byte[], byte[]> poll(long l) {
        return poll(Duration.ofMillis(l));
    }

    @Override
    public void commitSync(Map<TopicPartition, OffsetAndMetadata> map) {
    }

    @Override
    public void commitSync(Map<TopicPartition, OffsetAndMetadata> map, Duration duration) {
    }

    @Override
    public void commitAsync(
            Map<TopicPartition, OffsetAndMetadata> map, OffsetCommitCallback offsetCommitCallback) {
    }

    @Override
    public void commitSync() {
        commitSync(INFINITE_DURATION);
    }

    @Override
    public void commitSync(Duration duration) {
    }

    @Override
    public void commitAsync(OffsetCommitCallback offsetCommitCallback) {
    }

    @Override
    public void commitAsync() {
    }

    @Override
    public void seek(TopicPartition topicPartition, long l) {
        this.position = l;
    }

    @Override
    public void seek(TopicPartition topicPartition, OffsetAndMetadata offsetAndMetadata) {
        seek(topicPartition, offsetAndMetadata.offset());
    }

    @Override
    public void seekToBeginning(Collection<TopicPartition> collection) {
        this.position = STARTING_OFFSET;
    }

    @Override
    public void seekToEnd(Collection<TopicPartition> collection) {
    }

    @Override
    public long position(TopicPartition topicPartition) {
        return position(topicPartition, INFINITE_DURATION);
    }

    @Override
    public long position(TopicPartition partition, Duration timeout) {
        return this.position;
    }

    @Override
    public OffsetAndMetadata committed(TopicPartition topicPartition) {
        return committed(topicPartition, INFINITE_DURATION);
    }

    @Override
    public OffsetAndMetadata committed(TopicPartition topicPartition, Duration duration) {
        return committed(ImmutableSet.of(topicPartition), duration).get(topicPartition);
    }

    @Override
    public Map<TopicPartition, OffsetAndMetadata> committed(Set<TopicPartition> set) {
        return committed(set, INFINITE_DURATION);
    }

    @Override
    public Map<TopicPartition, OffsetAndMetadata> committed(
            Set<TopicPartition> partitions, Duration timeout) {
        ImmutableMap.Builder<TopicPartition, OffsetAndMetadata> output = ImmutableMap.builder();
        output.put(TOPIC_PARTITION, new OffsetAndMetadata(this.position));
        return output.build();
    }

    @Override
    public Map<MetricName, ? extends Metric> metrics() {
        return ImmutableMap.of();
    }

    @Override
    public List<PartitionInfo> partitionsFor(String s) {
        return partitionsFor(s, INFINITE_DURATION);
    }

    @Override
    public List<PartitionInfo> partitionsFor(String topic, Duration timeout) {
        return ImmutableList.of();
    }

    @Override
    public Map<String, List<PartitionInfo>> listTopics() {
        return listTopics(INFINITE_DURATION);
    }

    @Override
    public Map<String, List<PartitionInfo>> listTopics(Duration timeout) {
        return ImmutableMap.of();
    }

    @Override
    public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long> map) {
        return offsetsForTimes(map, INFINITE_DURATION);
    }

    @Override
    public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(
            Map<TopicPartition, Long> map, Duration duration) {
        throw new UnsupportedVersionException(
                "Pub/Sub Lite does not support Consumer backlog introspection.");
    }

    @Override
    public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> collection) {
        return beginningOffsets(collection, INFINITE_DURATION);
    }

    @Override
    public Map<TopicPartition, Long> beginningOffsets(
            Collection<TopicPartition> collection, Duration duration) {
        return ImmutableMap.of(TOPIC_PARTITION, STARTING_OFFSET);
    }

    @Override
    public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> collection) {
        return endOffsets(collection, INFINITE_DURATION);
    }

    @Override
    public Map<TopicPartition, Long> endOffsets(
            Collection<TopicPartition> collection, Duration duration) {
        return ImmutableMap.of(TOPIC_PARTITION, ENDING_OFFSET);
    }

    @Override
    public void close() {
        close(INFINITE_DURATION);
    }

    private static Duration toDuration(long l, TimeUnit timeUnit) {
        return Duration.ofMillis(TimeUnit.MILLISECONDS.convert(l, timeUnit));
    }

    @Override
    public void close(long l, TimeUnit timeUnit) {
        close(toDuration(l, timeUnit));
    }

    @Override
    public void close(Duration timeout) {
    }

    @Override
    public ConsumerGroupMetadata groupMetadata() {
        return new ConsumerGroupMetadata("/projects/12345/locations/us-central1/subscriptions/test-sub");
    }

    @Override
    public Set<TopicPartition> paused() {
        return ImmutableSet.of();
    }

    @Override
    public void pause(Collection<TopicPartition> collection) {
    }

    @Override
    public void resume(Collection<TopicPartition> collection) {
    }

    @Override
    public void enforceRebalance() {
    }

    @Override
    public void wakeup() {
    }

}
