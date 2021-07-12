package app.util;

import static com.google.common.truth.Truth.assertThat;
import static java.util.Arrays.asList;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.SneakyThrows;
import lombok.extern.log4j.Log4j2;
import org.junit.jupiter.api.Test;

@Log4j2
public class ScheduleManagerTest {

  @Test
  @SneakyThrows
  public void schedule() {
    long now = System.currentTimeMillis();
    Map<String,Long> m = new LinkedHashMap<>();
    ScheduleManager<String> sm = ScheduleManager.<String>builder()
        .taskRunner((models, self) -> {
          for (String model : models) {
            m.put(model, System.currentTimeMillis() - now);
          }
        })
        .build();
    int charCount = 10;
    long baseDelay = 100L;
    long interval = 50L;
    for (int i=0; i<charCount; i++) {
      sm.schedule(String.valueOf((char)('a' + i)), now + baseDelay + (charCount - i - 1) * interval);
    }
    Thread.sleep(baseDelay + (charCount - 1) * interval + 500L);
    List<String> keys = new ArrayList<>(m.keySet());
    List<String> expected = IntStream.range(0, charCount)
        .mapToObj(n -> String.valueOf((char)('a' + charCount - 1 - n)))
        .collect(Collectors.toList());
    while (sm.getStats().getActiveModelCount() > 0) {
      Thread.sleep(1000L);
    }
    log.info("m={}, stats={}", m, sm.getStats());
    assertThat(keys).isEqualTo(expected);
    assertThat(sm.getStats().getScheduleCount()).isEqualTo(charCount);
    assertThat(sm.getStats().getUnscheduleCount()).isEqualTo(0L);
    assertThat(sm.getStats().getExecutionCount()).isEqualTo(charCount);
    assertThat(sm.getStats().getActiveScheduleCount()).isEqualTo(0L);
    assertThat(sm.getStats().getActiveModelCount()).isEqualTo(0L);
    assertMapEntryIntervals(m, interval, 20L);
  }

  @Test
  @SneakyThrows
  public void reschedule()  {
    long now = System.currentTimeMillis();
    Map<String,Long> m = new LinkedHashMap<>();
    ScheduleManager<String> sm = ScheduleManager.<String>builder()
        .taskRunner((models, self) -> {
          for (String model : models) {
            m.put(model, System.currentTimeMillis() - now);
          }
        })
        .build();
    sm.schedule("a", now + 50L);
    sm.schedule("a", now + 100L);
    sm.schedule("b", now + 100L);
    sm.schedule("a", now + 200L);
    Thread.sleep(500L);
    List<String> keys = new ArrayList<>(m.keySet());
    List<String> expected = asList("b", "a");
    log.info("m={}, stats={}", m, sm.getStats());
    assertThat(keys).isEqualTo(expected);
    assertThat(sm.getStats().getScheduleCount()).isEqualTo(4L);
    assertThat(sm.getStats().getUnscheduleCount()).isEqualTo(2L);
    assertThat(sm.getStats().getExecutionCount()).isEqualTo(2L);
    assertThat(sm.getStats().getActiveScheduleCount()).isEqualTo(0L);
    assertThat(sm.getStats().getActiveModelCount()).isEqualTo(0L);
    assertMapEntryIntervals(m, 100L, 20L);
  }

  @Test
  @SneakyThrows
  public void cancelSchedule() {
    long now = System.currentTimeMillis();
    Map<String,Long> m = new LinkedHashMap<>();
    ScheduleManager<String> sm = ScheduleManager.<String>builder()
        .taskRunner((models, self) -> {
          for (String model : models) {
            m.put(model, System.currentTimeMillis() - now);
          }
        })
        .build();
    sm.schedule("a", now + 100L);
    sm.schedule("b", now + 100L);
    sm.schedule("c", now + 100L);
    Thread.sleep(30L);
    sm.unschedule("b");
    Thread.sleep(30L);
    sm.unschedule("a");
    sm.unschedule("b");
    Thread.sleep(100L);
    List<String> keys = new ArrayList<>(m.keySet());
    List<String> expected = asList("c");
    log.info("m={}, stats={}", m, sm.getStats());
    assertThat(keys).isEqualTo(expected);
    assertThat(sm.getStats().getScheduleCount()).isEqualTo(3L);
    assertThat(sm.getStats().getUnscheduleCount()).isEqualTo(2L);
    assertThat(sm.getStats().getExecutionCount()).isEqualTo(1L);
    assertThat(sm.getStats().getActiveScheduleCount()).isEqualTo(0L);
    assertThat(sm.getStats().getActiveModelCount()).isEqualTo(0L);
    assertMapEntryIntervals(m, 100L, 20L);
  }

  @SneakyThrows
  @Test
  public void cancelInvalidSchedule() {
    long now = System.currentTimeMillis();
    Map<String,Long> m = new LinkedHashMap<>();
    ScheduleManager<String> sm = ScheduleManager.<String>builder()
        .taskRunner((models, self) -> {
          for (String model : models) {
            m.put(model, System.currentTimeMillis() - now);
          }
        })
        .build();
    assertThat(sm.unschedule("a")).isFalse();
    assertThat(m).isEmpty();
    assertThat(sm.getStats().getScheduleCount()).isEqualTo(0L);
    assertThat(sm.getStats().getUnscheduleCount()).isEqualTo(0L);
    assertThat(sm.getStats().getExecutionCount()).isEqualTo(0L);
    assertThat(sm.getStats().getActiveScheduleCount()).isEqualTo(0L);
    assertThat(sm.getStats().getActiveModelCount()).isEqualTo(0L);
  }

  @Test
  @SneakyThrows
  public void repeatSchedule() {
    long now = System.currentTimeMillis();
    Map<String,Long> m = new LinkedHashMap<>();
    ScheduleManager<String> sm = ScheduleManager.<String>builder()
        .taskRunner((models, self) -> {
          for (String model : models) {
            m.put(model, System.currentTimeMillis() - now);
          }
        })
        .build();
    sm.schedule("a", now + 100L);
    sm.schedule("b", now + 100L);
    Thread.sleep(150L);
    sm.schedule("b", now + 200L);
    Thread.sleep(100L);
    List<String> keys = new ArrayList<>(m.keySet());
    List<String> expected = asList("a", "b");
    log.info("m={}, stats={}", m, sm.getStats());
    assertThat(keys).isEqualTo(expected);
    assertThat(sm.getStats().getScheduleCount()).isEqualTo(3L);
    assertThat(sm.getStats().getUnscheduleCount()).isEqualTo(0L);
    assertThat(sm.getStats().getExecutionCount()).isEqualTo(3L);
    assertThat(sm.getStats().getActiveScheduleCount()).isEqualTo(0L);
    assertThat(sm.getStats().getActiveModelCount()).isEqualTo(0L);
    assertMapEntryIntervals(m, 100L, 20L);
  }

  @Test
  @SneakyThrows
  public void duplicateSchedule() {
    long now = System.currentTimeMillis();
    Map<String,Long> m = new LinkedHashMap<>();
    ScheduleManager<String> sm = ScheduleManager.<String>builder()
        .taskRunner((models, self) -> {
          for (String model : models) {
            m.put(model, System.currentTimeMillis() - now);
          }
        })
        .build();
    sm.schedule("a", now + 100L);
    sm.schedule("b", now + 100L);
    sm.schedule("a", now + 100L);
    sm.schedule("b", now + 100L);
    Thread.sleep(150L);
    List<String> keys = new ArrayList<>(m.keySet());
    List<String> expected = asList("a", "b");
    log.info("m={}, stats={}", m, sm.getStats());
    assertThat(keys).isEqualTo(expected);
    assertThat(sm.getStats().getScheduleCount()).isEqualTo(2L);
    assertThat(sm.getStats().getUnscheduleCount()).isEqualTo(0L);
    assertThat(sm.getStats().getExecutionCount()).isEqualTo(2L);
    assertThat(sm.getStats().getActiveScheduleCount()).isEqualTo(0L);
    assertThat(sm.getStats().getActiveModelCount()).isEqualTo(0L);
    assertMapEntryIntervals(m, 0L, 20L);
  }

  @Test
  @SneakyThrows
  public void immediateExecute() {
    long now = System.currentTimeMillis();
    Map<String,Long> m = new LinkedHashMap<>();
    ScheduleManager<String> sm = ScheduleManager.<String>builder()
        .taskRunner((models, self) -> {
          for (String model : models) {
            m.put(model, System.currentTimeMillis() - now);
          }
        })
        .build();
    sm.schedule("a", now + 100L);
    sm.schedule("b", now - 100L);
    sm.schedule("c", now - 200L);
    Thread.sleep(150L);
    List<String> keys = new ArrayList<>(m.keySet());
    List<String> expected = asList("b", "c", "a");
    log.info("m={}, stats={}", m, sm.getStats());
    assertThat(keys).isEqualTo(expected);
    assertThat(sm.getStats().getScheduleCount()).isEqualTo(1L);
    assertThat(sm.getStats().getUnscheduleCount()).isEqualTo(0L);
    assertThat(sm.getStats().getExecutionCount()).isEqualTo(3L);
    assertThat(sm.getStats().getActiveScheduleCount()).isEqualTo(0L);
    assertThat(sm.getStats().getActiveModelCount()).isEqualTo(0L);
    assertMapEntryIntervals(m, 100L, 120L);
  }

  @Test
  @SneakyThrows
  public void recurringSchedule() {
    long now = System.currentTimeMillis();
    Map<String,Long> m = new LinkedHashMap<>();
    AtomicInteger recurringCount = new AtomicInteger();
    ScheduleManager<String> sm = ScheduleManager.<String>builder()
        .taskRunner((models, self) -> {
          long newNow = System.currentTimeMillis();
          for (String model : models) {
            m.put(model, newNow - now);
            if (recurringCount.getAndIncrement() < 2) {
              self.schedule(model, newNow + 100L);
            }
          }
        })
        .build();
    sm.schedule("a", now + 100L);
    Thread.sleep(500L);
    List<String> keys = new ArrayList<>(m.keySet());
    List<String> expected = asList("a");
    log.info("m={}, stats={}", m, sm.getStats());
    assertThat(keys).isEqualTo(expected);
    assertThat(sm.getStats().getScheduleCount()).isEqualTo(3L);
    assertThat(sm.getStats().getUnscheduleCount()).isEqualTo(0L);
    assertThat(sm.getStats().getExecutionCount()).isEqualTo(3L);
    assertThat(sm.getStats().getActiveScheduleCount()).isEqualTo(0L);
    assertThat(sm.getStats().getActiveModelCount()).isEqualTo(0L);
    assertMapEntryIntervals(m, 100L, 120L);
  }

  private void assertMapEntryIntervals(Map<String,Long> m, long entryInterval, long expectedMaxDiff) {
    Iterator<Entry<String,Long>> itr = m.entrySet().iterator();
    Entry<String,Long> curEntry = null;
    Entry<String,Long> prevEntry = null;
    long maxDiff = 0L;
    while (itr.hasNext()) {
      curEntry = itr.next();
      if (curEntry != null && prevEntry != null) {
        long diff = Math.abs(curEntry.getValue() - prevEntry.getValue() - entryInterval);
        if (diff > maxDiff) {
          maxDiff = diff;
        }
        assertThat(diff).isLessThan(expectedMaxDiff);
      }
      prevEntry = curEntry;
    }
    log.info("max diff = {}", maxDiff);
  }

}
