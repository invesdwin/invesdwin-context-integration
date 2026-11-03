package de.invesdwin.context.integration.ws.internal;

import java.io.IOException;

import javax.annotation.concurrent.ThreadSafe;

import org.springframework.web.bind.annotation.RequestMapping;

import de.invesdwin.context.integration.retry.RetryLaterPendingRuntimeException;
import de.invesdwin.util.collections.iterable.ICloseableIterable;
import jakarta.servlet.ServletOutputStream;
import jakarta.servlet.http.HttpServletResponse;

/**
 * Test spring-web controller activated by {@link RetryLaterFilterTest}; it is only annotated with @RequestMapping
 * (not @Controller) so that it is never picked up by the automatic component scan and instead becomes a bean only
 * while the test activates it - XmlRequestMappingHandlerMapping maps such @RequestMapping-only beans.
 */
@ThreadSafe
@RequestMapping(RetryLaterFilterTest.PATH)
public class TestRetryLaterController {

    public static final String PENDING_MESSAGE = RetryLaterPendingRuntimeException.PENDING_TAG
            + "Service is currently initializing: test";

    @RequestMapping("/refusal")
    public void refusal() {
        //the refusal-before shape: the handler method refuses while the service loads; on a request thread the
        //retry aspects never loop (the @RequestMapping methods run retry-disabled), so this leaves the handler
        //on every request
        throw new RetryLaterPendingRuntimeException(PENDING_MESSAGE);
    }

    @RequestMapping("/stream")
    public void stream(final HttpServletResponse response) throws IOException {
        //the refusal-during-stream-write shape: the controller already opened response.getOutputStream() and
        //the refusal surfaces later, while the writer iterates the demand (getBars/getTicks streaming endpoints)
        final ServletOutputStream out = response.getOutputStream();
        final ICloseableIterable<String> demand = () -> {
            throw new RetryLaterPendingRuntimeException(PENDING_MESSAGE);
        };
        for (final String element : demand) {
            out.print(element);
        }
    }

}
