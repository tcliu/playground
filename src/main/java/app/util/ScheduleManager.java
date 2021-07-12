package app.util;

import static java.util.Collections.singleton;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;
import lombok.Builder;
import lombok.Data;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import lombok.extern.log4j.Log4j2;
import org.agrona.collections.Object2LongHashMap;
import reactor.core.Disposable;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

@Log4j2
public class ScheduleManager<T> {

  public static final LongSupplier DEFAULT_CURRENT_TIMESTAMP_MS_GETTER = System::currentTimeMillis;

  public static final long MAX_TIME_DIFF_MS = 1000L;

  private final ExecutorService executeModelQueueExecutor = Executors.newSingleThreadExecutor();

  @Getter
  private final SortedMap<Long, Set<T>> modelsPerScheduledTimestampMs = new ConcurrentSkipListMap<>();

  private final Object2LongHashMap<T> scheduledTimestampMsPerModel = new Object2LongHashMap<>(0L);

  private final BlockingQueue<Collection<T>> executeModelQueue = new LinkedBlockingQueue<>();

  @Getter
  private final Stats stats = new Stats();

  private final TaskRunner<T> taskRunner;

  private final ScheduleHandler scheduleHandler;

  private final LongSupplier currentTimestampMsGetter;

  @Builder
  public ScheduleManager(TaskRunner<T> taskRunner, ScheduleHandler scheduleHandler,
      LongSupplier currentTimestampMsGetter) {
    Objects.requireNonNull(taskRunner, "Task runner cannot be null");
    this.taskRunner = taskRunner;
    this.scheduleHandler = scheduleHandler == null ?
        new ExecutorScheduleHandler() : scheduleHandler;
    this.currentTimestampMsGetter = currentTimestampMsGetter == null ?
        DEFAULT_CURRENT_TIMESTAMP_MS_GETTER : currentTimestampMsGetter;
    subscribeExpiredModelQueue();
  }

  public Collection<T> getModels(long scheduledTimestampMs, boolean createIfAbsent) {
    return modelsPerScheduledTimestampMs.compute(scheduledTimestampMs,
        (k, v) -> v == null && createIfAbsent ? new HashSet<>() : v);
  }

  private void subscribeExpiredModelQueue() {
    executeModelQueueExecutor.submit(() -> {
      for (;;) {
        try {
          Collection<T> executeModels = executeModelQueue.take();
          taskRunner.execute(executeModels, this);
          stats.executionCount += executeModels.size();
          stats.lastExecutionTimestampMs = currentTimestampMsGetter.getAsLong();
          log.info("Executed {}", stats.lastExecutionTimestampMs,
              executeModels);
        } catch (Throwable e) {
          e.printStackTrace();
        }
      }
    });
  }

  public ScheduleManager<T> schedule(T model, long scheduledTimestampMs) {
    long currentTimestampMs = currentTimestampMsGetter.getAsLong();
    long scheduledScheduledTimestampMs = scheduledTimestampMsPerModel.getValue(model);
    long delayMs = scheduledTimestampMs - currentTimestampMs;
    if (scheduledScheduledTimestampMs > 0L) {
      if (scheduledScheduledTimestampMs == scheduledTimestampMs) {
        log.info("Already scheduled - {}", model);
        return this;
      } else {
        unschedule(model);
      }
    }
    if (delayMs > 0L) {
      boolean reschedule = scheduleHandler.isScheduled()
          && !modelsPerScheduledTimestampMs.isEmpty()
          && modelsPerScheduledTimestampMs.firstKey() > scheduledTimestampMs;
      getModels(scheduledTimestampMs, true).add(model);
      scheduledTimestampMsPerModel.put(model, scheduledTimestampMs);
      stats.scheduleCount++;
      stats.lastScheduleTimestampMs = currentTimestampMs;
      if (reschedule) {
        scheduleHandler.cancelScheduler();
        if (log.isDebugEnabled()) {
          log.debug("Rescheduled to {}", scheduledTimestampMs);
        }
      }
      log.info("Scheduled [{}]", model);
      if (!scheduleHandler.isScheduled()) {
        schedule(delayMs);
      }
    } else {
      queueExecuteModels(singleton(model));
    }
    return this;
  }

  public boolean unschedule(T model) {
    long scheduledScheduledTimestampMs = scheduledTimestampMsPerModel.removeKey(model);
    Collection<T> models = getModels(scheduledScheduledTimestampMs, false);
    boolean removed = false;
    if (models != null) {
      removed = models.remove(model);
      if (models.isEmpty()) {
        modelsPerScheduledTimestampMs.remove(scheduledScheduledTimestampMs);
      }
    }
    if (removed) {
      if (modelsPerScheduledTimestampMs.isEmpty() && scheduleHandler.isScheduled()) {
        scheduleHandler.cancelScheduler();
      }
      stats.unscheduleCount++;
      stats.lastUnscheduleTimestampMs = currentTimestampMsGetter.getAsLong();
      log.info("Unscheduled [{}]", model);
    }
    return removed;
  }

  private void schedule(long delayMs) {
    scheduleHandler.schedule(this::execute, delayMs);
  }
  
  private void execute() {
    long currentTimestampMs = currentTimestampMsGetter.getAsLong();
    Iterator<Entry<Long, Set<T>>> entryItr = modelsPerScheduledTimestampMs.entrySet().iterator();
    while (entryItr.hasNext()) {
      Entry<Long, Set<T>> entry = entryItr.next();
      long diffMs = currentTimestampMs - entry.getKey();
      if (diffMs >= 0L) {
        if (diffMs >= MAX_TIME_DIFF_MS) {
          log.warn("Current timestamp is ahead of scheduled timestamp by {} ms", diffMs);
        }
        Collection<T> executeModels = entry.getValue();
        queueExecuteModels(executeModels);
        for (T executeModel : executeModels) {
          scheduledTimestampMsPerModel.removeKey(executeModel);
        }
        entryItr.remove();
      } else {
        break;
      }
    }
    if (!modelsPerScheduledTimestampMs.isEmpty()) {
      long nextScheduledTimestampMs = modelsPerScheduledTimestampMs.firstKey();
      long delayMs = nextScheduledTimestampMs - currentTimestampMs;
      schedule(delayMs);
    }
  }

  private void queueExecuteModels(Collection<T> executeModels) {
    if (!executeModelQueue.offer(executeModels)) {
      log.error("Failed to put models to execute model queue - {}", executeModels);
    }
  }

  @Data
  public class Stats {
    private long scheduleCount;
    private long unscheduleCount;
    private long executionCount;

    @ToString.Include
    public int getActiveScheduleCount() {
      return modelsPerScheduledTimestampMs.size();
    }

    @ToString.Include
    public int getActiveModelCount() {
      return scheduledTimestampMsPerModel.size();
    }

    private long lastScheduleTimestampMs;
    private long lastUnscheduleTimestampMs;
    private long lastExecutionTimestampMs;
  }

  @FunctionalInterface
  public interface TaskRunner<T> {

    void execute(Collection<T> models, ScheduleManager<T> scheduleManager) throws Exception;

  }

  public interface ScheduleHandler {

    void schedule(Runnable task, long delayMs);

    boolean cancelScheduler();

    boolean isScheduled();

  }

  @RequiredArgsConstructor
  public static class ExecutorScheduleHandler implements ScheduleHandler {

    private final ScheduledExecutorService scheduledExecutorService;

    private ScheduledFuture<?> scheduledFuture;

    public ExecutorScheduleHandler() {
      this(Executors.newSingleThreadScheduledExecutor());
    }

    @Override
    public void schedule(Runnable task, long delayMs) {
      scheduledFuture = scheduledExecutorService.schedule(task, delayMs, TimeUnit.MILLISECONDS);
    }

    @Override
    public boolean cancelScheduler() {
      boolean cancel = scheduledFuture != null;
      if (cancel) {
        scheduledFuture.cancel(true);
        scheduledFuture = null;
      }
      return cancel;
    }

    @Override
    public boolean isScheduled() {
      return scheduledFuture != null
          && !scheduledFuture.isCancelled()
          && !scheduledFuture.isDone();
    }

  }

  @RequiredArgsConstructor
  public static class ReactorScheduleHandler implements ScheduleHandler {

    private final Scheduler scheduler;

    private Disposable disposable;

    public ReactorScheduleHandler() {
      this(Schedulers.newSingle("reactor-scheduler-handler"));
    }

    @Override
    public void schedule(Runnable task, long delayMs) {
      scheduler.schedule(task, delayMs, TimeUnit.MILLISECONDS);
    }

    @Override
    public boolean cancelScheduler() {
      boolean cancel = disposable != null;
      if (cancel) {
        disposable.dispose();
      }
      return cancel;
    }

    @Override
    public boolean isScheduled() {
      return disposable != null && !disposable.isDisposed();
    }
  }

}
