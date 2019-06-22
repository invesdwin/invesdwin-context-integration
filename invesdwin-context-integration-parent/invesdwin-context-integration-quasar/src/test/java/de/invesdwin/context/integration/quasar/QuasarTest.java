package de.invesdwin.context.integration.quasar;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.Test;

import co.paralleluniverse.common.monitoring.MonitorType;
import co.paralleluniverse.fibers.FiberForkJoinScheduler;
import co.paralleluniverse.fibers.FiberScheduler;
import co.paralleluniverse.fibers.SuspendExecution;
import co.paralleluniverse.strands.SuspendableCallable;
import de.invesdwin.context.log.error.Err;
import de.invesdwin.context.test.ATest;
import de.invesdwin.util.concurrent.Executors;
import de.invesdwin.util.concurrent.WrappedExecutorService;
import de.invesdwin.util.concurrent.future.Futures;
import de.invesdwin.util.lang.ProcessedEventsRateString;
import de.invesdwin.util.math.Integers;
import de.invesdwin.util.time.Instant;
import de.invesdwin.util.time.duration.Duration;
import de.invesdwin.util.time.fdate.FTimeUnit;

@NotThreadSafe
public class QuasarTest extends ATest {

    private static final int REPETITIONS = 1;
    private static final int THREADS_COUNT = Executors.getCpuThreadPoolCount();
    private static final int STRATEGY_COUNT = 1000;
    private static final int CHUNK_SIZE = Integers.max(1, STRATEGY_COUNT / THREADS_COUNT / 10);
    private static final int TASK_COUNT = STRATEGY_COUNT / CHUNK_SIZE;

    private static final FiberScheduler FIBER_SCHEDULER = new FiberForkJoinScheduler(
            QuasarTest.class.getSimpleName() + "_FIBER", THREADS_COUNT, Err.UNCAUGHT_EXCEPTION_HANDLER,
            MonitorType.NONE, false);
    private static final WrappedExecutorService THREAD_SCHEDULER = Executors
            .newFixedThreadPool(QuasarTest.class.getSimpleName() + "_THREAD", THREADS_COUNT);

    @Test
    public void test() throws InterruptedException {
        for (int i = 1; i <= REPETITIONS; i++) {
            log.info("***************************** " + i);
            FTimeUnit.SECONDS.sleep(1);
            testSingle();
            FTimeUnit.SECONDS.sleep(1);
            testThreads();
            FTimeUnit.SECONDS.sleep(1);
            testFibers();
        }
    }

    private void testSingle() {
        final Instant start = new Instant();
        long ticksCount = 0;
        long stratsCount = 0;
        for (int i = 0; i < STRATEGY_COUNT; i++) {
            final long ticks = processTicks();
            ticksCount += ticks;
            stratsCount++;
        }
        final Duration duration = start.toDuration();
        log.info("single[1 threads * 1 tasks * " + STRATEGY_COUNT + " chunksize] ticks: " + ticksCount + " "
                + new ProcessedEventsRateString(ticksCount, duration));
        log.info("single[1 threads * 1 tasks * " + STRATEGY_COUNT + " chunksize] strats: " + stratsCount + " "
                + new ProcessedEventsRateString(stratsCount, duration));
    }

    private void testThreads() throws InterruptedException {
        final Instant start = new Instant();
        long ticksCount = 0;
        long stratsCount = 0;
        final List<Future<Long>> futures = new ArrayList<Future<Long>>();
        for (int i = 0; i < TASK_COUNT; i++) {
            futures.add(new ThreadTask(CHUNK_SIZE).start());
        }
        while (!futures.isEmpty()) {
            final Future<Long> removed = futures.remove(0);
            final long ticks = Futures.get(removed);
            ticksCount += ticks;
            stratsCount += CHUNK_SIZE;
        }
        final Duration duration = start.toDuration();
        log.info("threads[" + THREADS_COUNT + " threads * " + TASK_COUNT + " tasks * " + CHUNK_SIZE
                + " chunksize] ticks: " + ticksCount + " " + new ProcessedEventsRateString(ticksCount, duration));
        log.info("threads[" + THREADS_COUNT + " threads * " + TASK_COUNT + " tasks * " + CHUNK_SIZE
                + " chunksize] strats: " + stratsCount + " " + new ProcessedEventsRateString(stratsCount, duration));
    }

    private void testFibers() throws InterruptedException {
        final Instant start = new Instant();
        long ticksCount = 0;
        long stratsCount = 0;
        final List<Future<Long>> futures = new ArrayList<Future<Long>>();
        for (int i = 0; i < TASK_COUNT; i++) {
            futures.add(new FiberTask(CHUNK_SIZE).start());
        }
        while (!futures.isEmpty()) {
            final Future<Long> removed = futures.remove(0);
            final long ticks = Futures.get(removed);
            ticksCount += ticks;
            stratsCount += CHUNK_SIZE;
        }
        final Duration duration = start.toDuration();
        log.info("fibers[" + THREADS_COUNT + " threads * " + TASK_COUNT + " tasks * " + CHUNK_SIZE
                + " chunksize] ticks: " + ticksCount + " " + new ProcessedEventsRateString(ticksCount, duration));
        log.info("fibers[" + THREADS_COUNT + " threads * " + TASK_COUNT + " tasks * " + CHUNK_SIZE
                + " chunksize] strats: " + stratsCount + " " + new ProcessedEventsRateString(stratsCount, duration));
    }

    private static long processTicks() {
        long ticks = 0;

        for (int i = 0; i < 1000000; i++) {
            ticks++;
        }

        return ticks;
    }

    private static final class ThreadTask implements Callable<Long> {

        private final int chunkSize;

        private ThreadTask(final int chunkSize) {
            this.chunkSize = chunkSize;
        }

        public Future<Long> start() {
            return THREAD_SCHEDULER.submit(this);
        }

        @Override
        public Long call() {
            long countTicks = 0;
            for (int i = 0; i < chunkSize; i++) {
                countTicks += processTicks();
            }
            return countTicks;
        }

    }

    private static final class FiberTask implements SuspendableCallable<Long> {

        private final int chunkSize;

        private FiberTask(final int chunkSize) {
            this.chunkSize = chunkSize;
        }

        public Future<Long> start() {
            return FIBER_SCHEDULER.newFiber(this).start();
        }

        @Override
        public Long run() throws SuspendExecution, InterruptedException {
            long countTicks = 0;
            for (int i = 0; i < chunkSize; i++) {
                countTicks += processTicks();
            }
            return countTicks;
        }

    }

}
