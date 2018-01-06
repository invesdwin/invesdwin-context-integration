package de.invesdwin.context.integration.jppf.client;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

import javax.annotation.concurrent.NotThreadSafe;

import org.jppf.client.monitoring.topology.TopologyDriver;
import org.jppf.client.monitoring.topology.TopologyNode;
import org.junit.Test;

import de.invesdwin.context.integration.jppf.topology.ATopologyVisitor;
import de.invesdwin.context.test.ATest;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.concurrent.Futures;
import de.invesdwin.util.time.fdate.FTimeUnit;

@NotThreadSafe
public class ConfiguredJPPFExecutorServiceTest extends ATest {

    public static final class RemoteCallable implements Callable<Integer>, Serializable {
        private final int i;

        public RemoteCallable(final int i) {
            this.i = i;
        }

        @Override
        public Integer call() throws Exception {
            //CHECKSTYLE:OFF
            System.out.println("+++++++++++++ " + i);
            //CHECKSTYLE:ON
            FTimeUnit.SECONDS.sleep(1);
            //CHECKSTYLE:OFF
            System.out.println("------------- " + i);
            //CHECKSTYLE:ON
            return i;
        }
    }

    private static final int TASKS_COUNT = 20;

    @Test
    public void test() throws InterruptedException {
        final ConfiguredJPPFExecutorService executor = new ConfiguredJPPFExecutorService(
                ConfiguredJPPFClient.getInstance());
        final List<Callable<Integer>> tasks = new ArrayList<>();
        for (int i = 0; i < TASKS_COUNT; i++) {
            final int finalI = i;
            tasks.add(new RemoteCallable(finalI));
        }
        final List<Future<Integer>> futures = executor.invokeAll(tasks);
        final List<Integer> results = Futures.get(futures);
        for (int i = 0; i < TASKS_COUNT; i++) {
            Assertions.checkEquals(results.get(i), i);
        }
    }

    @Test
    public void testTopologyVisitor() {
        new ATopologyVisitor() {
            @Override
            protected void visitNode(final TopologyNode node) {
                log.info("Node: %s", node);
            }

            @Override
            protected void visitDriver(final TopologyDriver driver) {
                log.info("Driver: %s", driver);
            }
        }.process(ConfiguredJPPFClient.getTopologyManager());
        log.info("NodesCount: %s", ConfiguredJPPFClient.getNodesCount());
        log.info("ProcessingThreadsCount: %s", ConfiguredJPPFClient.getProcessingThreadsCount());
    }

}
