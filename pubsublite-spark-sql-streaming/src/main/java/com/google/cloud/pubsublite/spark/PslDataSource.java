package com.google.cloud.pubsublite.spark;

import org.apache.spark.sql.sources.DataSourceRegister;
import org.apache.spark.sql.sources.v2.ContinuousReadSupport;
import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.apache.spark.sql.sources.v2.DataSourceV2;
import org.apache.spark.sql.sources.v2.reader.streaming.ContinuousReader;
import org.apache.spark.sql.types.StructType;

import java.util.Optional;

public class PslDataSource implements DataSourceV2, ContinuousReadSupport, DataSourceRegister {


  @Override
  public String shortName() {
    return "pubsublite";
  }

  @Override
  public ContinuousReader createContinuousReader(Optional<StructType> schema,
                                                 String checkpointLocation,
                                                 DataSourceOptions options) {
    if (schema.isPresent()) {
      throw new IllegalArgumentException("PubSub Lite uses fixed schema and custom schema is not allowed");
    }
    return new PslContinuousReader(PslDataSourceOptions.fromSparkDataSourceOptions(options));
  }
}
