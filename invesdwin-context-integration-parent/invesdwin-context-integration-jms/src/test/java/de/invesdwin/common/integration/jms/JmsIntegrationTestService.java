package de.invesdwin.common.integration.jms;

import java.util.concurrent.TimeUnit;

import javax.annotation.concurrent.ThreadSafe;

import org.springframework.integration.annotation.ServiceActivator;

import de.invesdwin.context.log.Log;
import de.invesdwin.util.assertions.Assertions;

@ThreadSafe
public class JmsIntegrationTestService implements IJmsIntegrationTestService {

    private volatile boolean requestVerarbeitet;
    private final Log log = new Log(this);

    public void waitForProcessedRequest() throws InterruptedException {
        while (!requestVerarbeitet) {
            TimeUnit.SECONDS.sleep(1);
        }
        requestVerarbeitet = false;
    }

    @ServiceActivator(inputChannel = HELLO_WORLD_REQUEST_CHANNEL + "In")
    @Override
    public void helloWorld(final String request) {
        Assertions.assertThat(request).isEqualTo(JmsIntegrationTest.HELLO_WORLD);
        log.info(request);
        requestVerarbeitet = true;
    }

    @ServiceActivator(inputChannel = HELLO_WORLD_WITH_ANSWER_REQUEST_CHANNEL + "In")
    @Override
    public String helloWorldWithAnswer(final String request) {
        Assertions.assertThat(request).isEqualTo(JmsIntegrationTest.HELLO_WORLD);
        log.info(request);
        requestVerarbeitet = true;
        return request;
    }
}
