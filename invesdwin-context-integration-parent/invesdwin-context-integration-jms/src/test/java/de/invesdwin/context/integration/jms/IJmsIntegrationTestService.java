package de.invesdwin.context.integration.jms;

import org.springframework.integration.annotation.Gateway;
import org.springframework.stereotype.Service;

@Service
public interface IJmsIntegrationTestService {

    String HELLO_WORLD_REQUEST_CHANNEL = "helloWorld";
    String HELLO_WORLD_WITH_ANSWER_REQUEST_CHANNEL = "helloWorldWithAnswer";

    @Gateway(requestChannel = HELLO_WORLD_REQUEST_CHANNEL + "Out")
    void helloWorld(String request);

    @Gateway(requestChannel = HELLO_WORLD_WITH_ANSWER_REQUEST_CHANNEL + "Out")
    String helloWorldWithAnswer(String request);

}
