/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.addthis.cronus;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import java.time.Duration;
import java.time.ZonedDateTime;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.AbstractService;
import com.google.common.util.concurrent.Service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The cron scheduler is responsible for scheduling cron patterns
 * for repeated execution. Use the public constructor or the builder
 * class for generating scheduler instances. See Javadoc documentation
 * on constructor for advice on whether to use the constructor
 * or the builder. Cron patterns are scheduled via the
 * {@code #schedule(CronPattern, Runnable, boolean)} method.
 * The cron scheduler is a {@link Service}.
 */
public class CronScheduler extends AbstractService implements Service {

    /**
     * The {@code CronFutureExternal} is returned on scheduling a pattern.
     * It extends from {@code CompletableFuture} for convenience of implementation
     * but the return type exposed to the user is that of a {@code Future}. Internally
     * {@code CronFutureInternal} is used to maintain the state of a scheduled
     * pattern. Each scheduled pattern is tracked with two Futures:
     * {@code CronFutureInternal#current} is the next action that will be taken
     * by the pattern and {@code CronFutureInternal#next} is the next rescheduling
     * of the pattern. When a pattern action occurs, it first schedules it's
     * next occurrence before beginning to perform the action.
     */

    private static final Logger log = LoggerFactory.getLogger(CronScheduler.class);

    @Nonnull
    private final ScheduledExecutorService executor;

    /**
     * The lock does not guard the {@code preStartupFutures} object is the object
     * itself is thread safe. Rather the lock guards the reference to the object.
     * Acquiring the read lock grants read privileges to the reference and
     * allows for modification of the object. Acquiring the write lock is used
     * during {@link #doStart()} to set the reference to null.
     */
    @Nonnull
    private final ReadWriteLock preStartupFuturesLock;

    @Nullable
    @GuardedBy("preStartupFuturesLock")
    private ConcurrentHashMap<CronFutureExternal<?>, CronRunnable> preStartupFutures;

    @Nonnull
    private final ConcurrentHashMap<CronFutureExternal<?>, CronFutureInternal> futures;

    @Nonnull
    private final Duration shutdownWait;

    /**
     * Clients may either use this constructor to generate CronScheduler instances
     * or they may use the provided Builder class. Because (in our opinion) several
     * of the default parameters of the {@code ScheduledThreadPoolExecutor} class
     * are not what you want from a cron service we encourage you to use the
     * Builder class. It has default values set to what we think you want
     * them to be.
     */
    public CronScheduler(@Nonnull ScheduledExecutorService scheduledExecutorService,
                         @Nonnull Duration shutdownWait) {
        Preconditions.checkNotNull(scheduledExecutorService, "scheduledExecutorService argument must be non-null");
        Preconditions.checkNotNull(shutdownWait, "shutdownWait argument must be non-null");
        this.executor = scheduledExecutorService;
        this.futures = new ConcurrentHashMap<>();
        this.preStartupFutures = new ConcurrentHashMap<>();
        this.preStartupFuturesLock = new ReentrantReadWriteLock();
        this.shutdownWait = shutdownWait;
    }

    /**
     * Submits a cron pattern for execution. Any patterns submitted prior to startUp
     * are not executed until startUp is invoked. Patterns cannot be submitted after
     * the scheduler has been shutdown. This method returns a {@link Future} object.
     * Invoking {@link Future#cancel(boolean)} will cancel any future executions of this
     * pattern. Invoking {@link Future#get()} will block indefinitely until the
     * task throws an exception and only if {@code stopOnFailure} is true.
     */
    public Future<?> schedule(CronPattern pattern, Runnable runnable, boolean stopOnFailure) {
        Preconditions.checkNotNull(pattern, "pattern argument must be non-null");
        Preconditions.checkNotNull(runnable, "runnable argument must be non-null");
        CronFutureExternal<?> key = new CronFutureExternal<>();
        CronRunnable cronRunnable = new CronRunnable(pattern, runnable, key, stopOnFailure);
        preStartupFuturesLock.readLock().lock();
        try {
            if (preStartupFutures != null) {
                preStartupFutures.put(key, cronRunnable);
                return key;
            }
        } finally {
            preStartupFuturesLock.readLock().unlock();
        }
        futures.put(key, new CronFutureInternal(null, submitToExecutor(cronRunnable, true)));
        return key;
    }

    @Override
    protected void doStart() {
        ConcurrentHashMap<CronFutureExternal<?>, CronRunnable> map;
        log.info("Starting cron scheduler");
        preStartupFuturesLock.writeLock().lock();
        try {
            map = preStartupFutures;
            preStartupFutures = null;
        } finally {
            preStartupFuturesLock.writeLock().unlock();
        }
        if (map != null) {
            for (Map.Entry<CronFutureExternal<?>, CronRunnable> entry : map.entrySet()) {
                if (!entry.getKey().isCancelled()) {
                    futures.put(entry.getKey(),
                                new CronFutureInternal(null, submitToExecutor(entry.getValue(), true)));
                }
            }
        }
        notifyStarted();
    }

    @Override
    protected void doStop() {
        log.info("Stopping cron scheduler");
        executor.shutdown();
        try {
            executor.awaitTermination(shutdownWait.toNanos(), TimeUnit.NANOSECONDS);
        } catch (InterruptedException ex) {
            log.info("cron scheduler interrupted while waiting for shutdown");
        }
        executor.shutdownNow();
        notifyStopped();
    }

    public void start() {
        startAsync();
    }

    public void stop() {
        stopAsync();
    }

    public ScheduledExecutorService getExecutor() {
        return executor;
    }

    /**
     * Constructs a Cron scheduler. In this builder the default
     * behavior is to remove cancelled tasks from the scheduler queue
     * and not to wait for scheduled tasks when the executor is shut down.
     */
    public static class Builder {

        private final int corePoolSize;

        private Duration shutdownWait = Duration.ZERO;

        private ThreadFactory threadFactory = null;

        private RejectedExecutionHandler handler = null;

        private boolean removeOnCancel = true;

        private boolean continueAfterShutdown = false;

        public Builder(int corePoolSize) {
            this.corePoolSize = corePoolSize;
        }

        public Builder setShutdownWait(Duration shutdownWait) {
            Preconditions.checkNotNull(shutdownWait, "shutdownWait argument must be non-null");
            this.shutdownWait = shutdownWait;
            return this;
        }

        public Builder setThreadFactory(ThreadFactory threadFactory) {
            this.threadFactory = threadFactory;
            return this;
        }

        public Builder setHandler(RejectedExecutionHandler handler) {
            this.handler = handler;
            return this;
        }

        public Builder setRemoveOnCancel(boolean removeOnCancel) {
            this.removeOnCancel = removeOnCancel;
            return this;
        }

        public Builder setContinueAfterShutdown(boolean continueAfterShutdown) {
            this.continueAfterShutdown = continueAfterShutdown;
            return this;
        }

        public CronScheduler build() {
            ScheduledThreadPoolExecutor executor;
            if ((threadFactory == null) && (handler == null)) {
                executor = new ScheduledThreadPoolExecutor(corePoolSize);
            } else if (threadFactory == null) {
                executor = new ScheduledThreadPoolExecutor(corePoolSize, handler);
            } else if (handler == null) {
                executor = new ScheduledThreadPoolExecutor(corePoolSize, threadFactory);
            } else {
                executor = new ScheduledThreadPoolExecutor(corePoolSize, threadFactory, handler);
            }
            executor.setRemoveOnCancelPolicy(removeOnCancel);
            executor.setExecuteExistingDelayedTasksAfterShutdownPolicy(continueAfterShutdown);
            return new CronScheduler(executor, shutdownWait);
        }
    }

    private ScheduledFuture<?> submitToExecutor(CronRunnable cronRunnable, boolean inclusive) {
        ZonedDateTime now = ZonedDateTime.now();
        ZonedDateTime next = cronRunnable.pattern.next(now, inclusive);
        long delta;
        if (now.equals(next)) {
            delta = 0;
        } else {
            Duration difference = Duration.between(now, next);
            delta = difference.toNanos();
        }
        return executor.schedule(cronRunnable, delta, TimeUnit.NANOSECONDS);
    }

    private void cancel(CronFutureExternal<?> future, boolean mayInterruptIfRunning) {
        preStartupFuturesLock.readLock().lock();
        try {
            if (preStartupFutures != null) {
                CronRunnable cronRunnable = preStartupFutures.remove(future);
                if (cronRunnable != null) {
                    return;
                }
            }
        } finally {
            preStartupFuturesLock.readLock().unlock();
        }
        CronFutureInternal pair = futures.remove(future);
        if (pair != null) {
            if (pair.current != null) {
                pair.current.cancel(mayInterruptIfRunning);
            }
            if (pair.next != null) {
                pair.next.cancel(mayInterruptIfRunning);
            }
        }
    }

    private class CronFutureExternal<V> extends CompletableFuture<V> {

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            boolean result = super.cancel(mayInterruptIfRunning);
            CronScheduler.this.cancel(this, mayInterruptIfRunning);
            return result;
        }

    }

    private class CronRunnable implements Runnable {

        private final CronPattern pattern;
        private final Runnable runnable;
        private final CronFutureExternal<?> key;
        private final boolean stopOnFailure;

        CronRunnable(CronPattern pattern, Runnable runnable, CronFutureExternal<?> key, boolean stopOnFailure) {
            this.pattern = pattern;
            this.runnable = runnable;
            this.key = key;
            this.stopOnFailure = stopOnFailure;
        }

        @Override public void run() {
            CronFutureInternal reschedule = futures.compute(key, (key, prev) -> (
                    (prev != null) ?
                    new CronFutureInternal(
                            prev.next, submitToExecutor(this, false)) : null));

            if (reschedule != null) {
                try {
                    runnable.run();
                } catch (Exception ex) {
                    if (stopOnFailure) {
                        key.completeExceptionally(ex);
                        CronScheduler.this.cancel(key, false);
                    } else {
                        log.warn("Ignoring exception for pattern \"{}\": ", pattern.source, ex);
                    }
                }
            }
        }
    }

    private static class CronFutureInternal {
        private final ScheduledFuture<?> current;
        private final ScheduledFuture<?> next;

        CronFutureInternal(ScheduledFuture<?> current, ScheduledFuture<?> next) {
            this.current = current;
            this.next = next;
        }
    }

}