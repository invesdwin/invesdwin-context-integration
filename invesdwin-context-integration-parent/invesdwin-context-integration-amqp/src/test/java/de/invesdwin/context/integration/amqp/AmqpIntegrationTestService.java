package de.invesdwin.context.integration.amqp;

import java.util.concurrent.TimeUnit;

import javax.annotation.concurrent.ThreadSafe;

import org.springframework.integration.annotation.ServiceActivator;

import de.invesdwin.context.log.Log;
import de.invesdwin.util.assertions.Assertions;

@ThreadSafe
public class AmqpIntegrationTestService implements IAmqpIntegrationTestService {

    private volatile boolean requestProcessed;
    private final Log log = new Log(this);

    public void waitForProcessedRequest() throws InterruptedException {
        while (!requestProcessed) {
            TimeUnit.SECONDS.sleep(1);
        }
        requestProcessed = false;
    }

    @ServiceActivator(inputChannel = HELLO_WORLD_REQUEST_CHANNEL + "In")
    @Override
    public void helloWorld(final String request) {
        Assertions.assertThat(request).isEqualTo(AmqpIntegrationTest.HELLO_WORLD);
        log.info(request);
        requestProcessed = true;
    }

    @ServiceActivator(inputChannel = HELLO_WORLD_WITH_ANSWER_REQUEST_CHANNEL + "In")
    @Override
    public String helloWorldWithAnswer(final String request) {
        Assertions.assertThat(request).isEqualTo(AmqpIntegrationTest.HELLO_WORLD);
        log.info(request);
        requestProcessed = true;
        return request;
    }
}
