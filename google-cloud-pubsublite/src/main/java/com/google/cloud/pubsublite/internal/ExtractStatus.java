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

package com.google.cloud.pubsublite.internal;

import com.google.api.core.ApiFuture;
import com.google.common.util.concurrent.MoreExecutors;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

public final class ExtractStatus {
  public static Optional<Status> extract(Throwable t) {
    try {
      throw t;
    } catch (StatusException e) {
      return Optional.of(e.getStatus());
    } catch (StatusRuntimeException e) {
      return Optional.of(e.getStatus());
    } catch (Throwable e) {
      return Optional.empty();
    }
  }

  public static StatusException toCanonical(Throwable t) {
    Optional<Status> statusOr = extract(t);
    if (statusOr.isPresent()) return statusOr.get().asException();
    return Status.INTERNAL.withCause(t).asException();
  }

  public static void addFailureHandler(ApiFuture<?> future, Consumer<StatusException> consumer) {
    future.addListener(
        () -> {
          try {
            future.get();
          } catch (ExecutionException e) {
            consumer.accept(toCanonical(e.getCause()));
          } catch (InterruptedException e) {
            consumer.accept(toCanonical(e));
          }
        },
        MoreExecutors.directExecutor());
  }

  public interface StatusFunction<I, O> {
    O apply(I input) throws StatusException;
  }

  public interface StatusConsumer<I> {
    void apply(I input) throws StatusException;
  }

  public interface StatusBiconsumer<K, V> {
    void apply(K key, V value) throws StatusException;
  }

  public static <I, O> Function<I, O> rethrowAsRuntime(StatusFunction<I, O> function) {
    return i -> {
      try {
        return function.apply(i);
      } catch (StatusException e) {
        throw e.getStatus().asRuntimeException();
      }
    };
  }

  public static <I> Consumer<I> rethrowAsRuntime(StatusConsumer<I> consumer) {
    return i -> {
      try {
        consumer.apply(i);
      } catch (StatusException e) {
        throw e.getStatus().asRuntimeException();
      }
    };
  }

  public static <K, V> BiConsumer<K, V> rethrowAsRuntime(StatusBiconsumer<K, V> consumer) {
    return (k, v) -> {
      try {
        consumer.apply(k, v);
      } catch (StatusException e) {
        throw e.getStatus().asRuntimeException();
      }
    };
  }

  private ExtractStatus() {}
}
