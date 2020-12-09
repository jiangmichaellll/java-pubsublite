package com.google.cloud.pubsublite.spark;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.cloud.pubsublite.Offset;
import com.google.cloud.pubsublite.Partition;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public final class PslSourceOffset extends org.apache.spark.sql.sources.v2.reader.streaming.Offset
        implements Serializable {

  private static final ObjectMapper objectMapper = new ObjectMapper();

  static {
    objectMapper.configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true);
  }

  private final ImmutableMap<Partition, Offset> partitionOffsetMap;

  public PslSourceOffset(Map<Partition, Offset> map) {
    this.partitionOffsetMap = ImmutableMap.copyOf(map);
  }

  public static PslSourceOffset merge(PslSourceOffset o1, PslSourceOffset o2) {
    Map<Partition, Offset> result = new HashMap<>(o1.partitionOffsetMap);
    o2.partitionOffsetMap.forEach(
        (k, v) -> result.merge(k, v, (v1, v2) -> Collections.max(ImmutableList.of(v1, v2))));
    return new PslSourceOffset(result);
  }

  public static PslSourceOffset merge(PslPartitionOffset[] offsets) {
    Map<Partition, Offset> map = new HashMap<>();
    for (PslPartitionOffset po : offsets) {
      assert !map.containsKey(po.partition()) : "Multiple PslPartitionOffset has same partition.";
      map.put(po.partition(), po.offset());
    }
    return new PslSourceOffset(map);
  }

  public static PslSourceOffset fromJson(String json) {
    Map<String, Number> map;
    try {
      // TODO: Use TypeReference instead of Map.class, currently TypeReference breaks spark with
      // java.lang.LinkageError: loader constraint violation: loader previously initiated loading
      // for a different type.
      map = objectMapper.readValue(json, Map.class);
    } catch (IOException e) {
      throw new IllegalStateException("Unable to deserialize PslSourceOffset.", e);
    }
    Map<Partition, Offset> partitionOffsetMap =
        map.entrySet().stream()
            .collect(Collectors.toMap(e -> Partition.of(Long.parseLong(e.getKey())),
                    e -> Offset.of(e.getValue().longValue())));
    return new PslSourceOffset(partitionOffsetMap);
  }

  public Map<Partition, Offset> getPartitionOffsetMap() {
    return this.partitionOffsetMap;
  }

  @Override
  public String json() {
    try {
      Map<Long, Long> map =
          partitionOffsetMap.entrySet().stream()
              .collect(Collectors.toMap(e -> e.getKey().value(), e -> e.getValue().value()));
      return objectMapper.writeValueAsString(map);
    } catch (JsonProcessingException e) {
      throw new IllegalStateException("Unable to serialize PslSourceOffset.", e);
    }
  }
}
