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

    public static final String PENDING_MESSAGE = RetryLaterPendingRuntimeException.PENDING_PREFIX
            + "Service is currently initializing: test";
    public static final String OK_BODY = "retry-later-filter-test-body";
    //bigger than IntegrationWsProperties.SPRING_WEB_RETRY_LATER_BUFFER_BYTES (256 KB by default)
    public static final int LARGE_BODY_BYTES = 300 * 1024;
    public static final String LARGE_BODY = newLargeBody();

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

    /**
     * The shape the production defect had: a bean writer pushes its format header into the container (its close()
     * flushes) and the demand then refuses. Without RetryLaterBufferedResponse that flush commits the response, the
     * filter cannot answer any more and the webserver logs a WARN per request; with the guard the header is still
     * buffered, so the refusal becomes a retry-later status and the client gets its message.
     */
    @RequestMapping("/flushThenRefusal")
    public void flushThenRefusal(final HttpServletResponse response) throws IOException {
        final ServletOutputStream out = response.getOutputStream();
        out.print("format-header");
        out.flush();
        throw new RetryLaterPendingRuntimeException(PENDING_MESSAGE);
    }

    /**
     * The refusal after the body has grown past RetryLaterBufferedResponse's buffer: those bytes are already in the
     * container, so the honest behaviour is the pre-guard one - the client keeps the partial body and the refusal
     * does not become a retry-later status.
     */
    @RequestMapping("/largeThenRefusal")
    public void largeThenRefusal(final HttpServletResponse response) throws IOException {
        try (ServletOutputStream out = response.getOutputStream()) {
            out.print(LARGE_BODY);
            throw new RetryLaterPendingRuntimeException(PENDING_MESSAGE);
        }
    }

    /**
     * The success path must be untouched by the buffering guard: the body arrives complete and exactly once, also
     * when it is bigger than the buffer (that is where a premature release plus a second write would show up).
     */
    @RequestMapping("/largeOk")
    public void largeOk(final HttpServletResponse response) throws IOException {
        try (ServletOutputStream out = response.getOutputStream()) {
            out.print(LARGE_BODY);
        }
    }

    @RequestMapping("/ok")
    public void ok(final HttpServletResponse response) throws IOException {
        try (ServletOutputStream out = response.getOutputStream()) {
            out.print(OK_BODY);
        }
    }

    private static String newLargeBody() {
        final StringBuilder builder = new StringBuilder(LARGE_BODY_BYTES);
        for (int i = 0; i < LARGE_BODY_BYTES; i++) {
            builder.append('x');
        }
        return builder.toString();
    }

}
