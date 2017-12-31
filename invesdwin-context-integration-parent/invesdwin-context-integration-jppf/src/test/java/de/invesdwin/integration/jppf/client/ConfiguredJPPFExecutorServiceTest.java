package de.invesdwin.integration.jppf.client;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.Test;

import de.invesdwin.context.test.ATest;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.concurrent.Futures;
import de.invesdwin.util.time.fdate.FTimeUnit;

@NotThreadSafe
public class ConfiguredJPPFExecutorServiceTest extends ATest {

    private static final int TASKS_COUNT = 100;

    @Test
    public void test() throws InterruptedException {
        final ConfiguredJPPFExecutorService executor = new ConfiguredJPPFExecutorService(
                ConfiguredJPPFClient.getInstance());
        final List<Callable<Integer>> tasks = new ArrayList<>();
        for (int i = 0; i < TASKS_COUNT; i++) {
            final int finalI = i;
            tasks.add(new Callable<Integer>() {
                @Override
                public Integer call() throws Exception {
                    FTimeUnit.SECONDS.sleep(10);
                    return finalI;
                }
            });
        }
        final List<Future<Integer>> futures = executor.invokeAll(tasks);
        final List<Integer> results = Futures.get(futures);
        for (int i = 0; i < 100; i++) {
            Assertions.checkEquals(results.get(i), i);
        }
    }

}
