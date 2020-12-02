package com.google.cloud.pubsublite.spark;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.cloud.pubsublite.Partition;
import com.google.cloud.pubsublite.internal.wire.Committer;
import com.google.cloud.pubsublite.internal.wire.CommitterBuilder;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.flogger.GoogleLogger;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.errorprone.annotations.concurrent.GuardedBy;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

public class MultiPartitionCommitter implements Serializable {
    private static final GoogleLogger log = GoogleLogger.forEnclosingClass();

    private final PslDataSourceOptions options;
    private final CommitterFactory committerFactory;

    private final AtomicBoolean initialized = new AtomicBoolean(false);
    private final ReentrantLock commitLock = new ReentrantLock();
    @GuardedBy("commitLock")
    private final Map<Partition, Committer> committerMap = new HashMap<>(); // lazily constructed
    private static class CommitState {
        com.google.cloud.pubsublite.Offset offset;
        ApiFuture<Void> future;
        public CommitState(com.google.cloud.pubsublite.Offset offset, ApiFuture<Void> future) {
            this.offset = offset;
            this.future = future;
        }
    }
    @GuardedBy("commitLock")
    private final Map<Partition, CommitState> commitStates = new HashMap<>();

    public MultiPartitionCommitter(PslDataSourceOptions options) {
        this(options, (opt, partition) -> CommitterBuilder.newBuilder()
                .setSubscriptionPath(opt.subscriptionPath())
                .setPartition(partition)
                .setServiceClient(PslSparkUtils.getCursorClientFactory().newClient(opt)).build());
    }

    @VisibleForTesting
    public MultiPartitionCommitter(PslDataSourceOptions options, CommitterFactory committerFactory) {
        this.options = options;
        this.committerFactory = committerFactory;
    }

    private void init(Set<Partition> partitions) {
        partitions.forEach(p -> {
            Committer committer = committerFactory.newCommitter(options, p);
            committer.startAsync().awaitRunning();
            committerMap.put(p, committer);
        });
    }

    private void tryCleanCommitStates(Partition partition) {
        commitLock.lock();
        try {
            if (!commitStates.containsKey(partition)) {
                return;
            }
            if (commitStates.get(partition).future.isDone()) {
                commitStates.remove(partition);
            }
        } finally {
            commitLock.unlock();
        }
    }

    public void close() {
        commitLock.lock();
        try {
            commitStates.forEach((k,v) -> {
                try {
                    v.future.get(200, TimeUnit.MILLISECONDS);
                } catch (InterruptedException | ExecutionException | TimeoutException ex) {
                    log.atWarning().log("Failed to commit %s,%s.", k.value(), v.offset.value(), ex);
                }
            });
            committerMap.values().forEach(c -> c.stopAsync().awaitTerminated());
        } finally {
            commitLock.unlock();
        }
    }

    public void commit(PslSourceOffset offset) {
        if (!initialized.get()) {
            init(offset.getPartitionOffsetMap().keySet());
            initialized.set(true);
        }

        commitLock.lock();
        try {
            offset
                    .getPartitionOffsetMap()
                    .forEach(
                            (p, o) -> {
                                if (commitStates.containsKey(p)) {
                                    commitStates.get(p).future.cancel(true);
                                }
                                // TODO(jiangmichael): Adds a method in commitOffset to accept commitPool.
                                ApiFuture<Void> future = committerMap.get(p).commitOffset(o);
                                ApiFutures.addCallback(future, new ApiFutureCallback<Void>() {
                                    @Override
                                    public void onFailure(Throwable t) {
                                        if (!future.isCancelled()) {
                                            log.atWarning().log("Failed to commit %s,%s.", p.value(), o.value(), t);
                                        }
                                        tryCleanCommitStates(p);
                                    }

                                    @Override
                                    public void onSuccess(Void result) {
                                        log.atInfo().log("Committed %s,%s.", p.value(), o.value());
                                        tryCleanCommitStates(p);
                                    }
                                }, MoreExecutors.directExecutor());

                                commitStates.put(p, new CommitState(o, future));
                            });
        } finally {
            commitLock.unlock();
        }
    }

    public interface CommitterFactory extends Serializable {
        Committer newCommitter(PslDataSourceOptions options, Partition partition);
    }

}
