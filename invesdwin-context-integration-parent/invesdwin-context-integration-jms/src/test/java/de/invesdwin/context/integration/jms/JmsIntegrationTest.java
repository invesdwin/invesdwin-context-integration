package de.invesdwin.context.integration.jms;

import javax.annotation.concurrent.ThreadSafe;
import jakarta.inject.Inject;
import jakarta.inject.Named;

import org.junit.jupiter.api.Test;

import de.invesdwin.context.test.ATest;
import de.invesdwin.context.test.TestContext;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.assertions.Executable;
import de.invesdwin.util.time.date.FTimeUnit;
import de.invesdwin.util.time.duration.Duration;

@ThreadSafe
public class JmsIntegrationTest extends ATest {

    public static final String HELLO_WORLD = "Hello World";

    @Inject
    @Named("jmsIntegrationTestServiceGateway")
    private IJmsIntegrationTestService gateway;
    @Inject
    private JmsIntegrationTestService service;

    @Override
    public void setUpContext(final TestContext ctx) throws Exception {
        super.setUpContext(ctx);
        ctx.activateBean(JmsIntegrationTestContextLocation.class);
    }

    @Test
    public void testHelloWorld() throws InterruptedException {
        Assertions.assertTimeout(new Duration(3, FTimeUnit.SECONDS), new Executable() {
            @Override
            public void execute() throws Throwable {
                gateway.helloWorld(HELLO_WORLD);
                service.waitForProcessedRequest();
            }
        });
    }

    @Test
    public void testHelloWorldWithAnswer() throws InterruptedException {
        Assertions.assertTimeout(new Duration(3, FTimeUnit.SECONDS), new Executable() {
            @Override
            public void execute() throws Throwable {
                final String response = gateway.helloWorldWithAnswer(HELLO_WORLD);
                Assertions.assertThat(response).isEqualTo(HELLO_WORLD);
                log.info(response);
                service.waitForProcessedRequest(); //in fact useless here, but still checking
            }
        });
    }

}
