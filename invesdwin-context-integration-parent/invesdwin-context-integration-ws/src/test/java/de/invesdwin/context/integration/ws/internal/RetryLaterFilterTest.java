package de.invesdwin.context.integration.ws.internal;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import javax.annotation.concurrent.ThreadSafe;

import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;

import de.invesdwin.context.integration.IntegrationProperties;
import de.invesdwin.context.integration.ws.IntegrationWsProperties;
import de.invesdwin.context.test.ATest;
import de.invesdwin.context.test.ITestContextSetup;
import de.invesdwin.context.webserver.test.WebserverTest;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.error.Throwables;
import de.invesdwin.util.lang.uri.URIs;

/**
 * A retryable refusal from a spring-web handler during loading must surface as a clean retry-later HTTP status,
 * not as a servlet exception that the webserver logs per request (the 'HttpChannel.handleException' WARN flood
 * with 'HTTP ERROR 500 jakarta.servlet.ServletException: Request processing failed: ...RetryLater...' bodies).
 * Verifies the RetryLaterFilter registration of this module's web-fragment.
 */
@ThreadSafe
@WebserverTest
public class RetryLaterFilterTest extends ATest {

    static final String PATH = "/retrylatertest";

    @Override
    public void setUpContext(final ITestContextSetup ctx) throws Exception {
        ctx.activateBean(TestRetryLaterController.class);
    }

    @Test
    public void testRetryLaterRefusalBeforeIsNotAServletErrorPage() throws IOException {
        //the default body is the verdict line only: no stack, no servlet error page
        Assertions.assertThat(assertRetryLaterStatus(newUri("/refusal"))).doesNotContain("at de.invesdwin.");
    }

    /**
     * The dominant staging shape: the refusal surfaces while the writer iterates the demand - the controller has
     * already opened response.getOutputStream() at that point (the getBars/getTicks streaming endpoints).
     */
    @Test
    public void testRetryLaterDuringStreamWriteIsNotAServletErrorPage() throws IOException {
        assertRetryLaterStatus(newUri("/stream"));
    }

    /**
     * The load-bearing case: the handler pushed bytes into the container (its writer's close() flushes the format
     * header) and only then refuses. With the guard the header is still buffered, so the refusal is answered as a
     * retry-later status with its message; without it (SPRING_WEB_RETRY_LATER_BUFFER_BYTES=0) the flush has already
     * committed the response and this cannot hold any more.
     */
    @Test
    public void testRefusalAfterAHeaderFlushIsStillAnsweredAsRetryLater() throws IOException {
        final String body = assertRetryLaterStatus(newUri("/flushThenRefusal"));
        Assertions.assertThat(body).doesNotContain("format-header");
    }

    /**
     * The buffering guard must not change a successful response: the body arrives complete and exactly once, also
     * when it is bigger than the buffer (the shape where an early release followed by a second write of the prefix
     * would show up as a wrong length).
     */
    @Test
    public void testSuccessfulBodiesArriveCompleteAndExactlyOnce() throws IOException {
        Assertions.assertThat(new String(downloadBody(newUri("/ok")), StandardCharsets.UTF_8))
                .isEqualTo(TestRetryLaterController.OK_BODY);
        final byte[] large = downloadBody(newUri("/largeOk"));
        Assertions.assertThat(large).hasSize(TestRetryLaterController.LARGE_BODY_BYTES);
        Assertions.assertThat(new String(large, StandardCharsets.UTF_8))
                .isEqualTo(TestRetryLaterController.LARGE_BODY);
    }

    /**
     * The documented limit of the guard: once the body has overflowed the buffer it is in the container, so the
     * refusal cannot be replaced by a retry-later status - the client keeps its partial body instead.
     */
    @Test
    public void testRefusalAfterTheBufferOverflowKeepsThePartialBody() throws IOException {
        String failureMessage = null;
        int receivedBytes = 0;
        try {
            receivedBytes = downloadBody(newUri("/largeThenRefusal")).length;
        } catch (final IOException e) {
            failureMessage = e.getMessage();
        }
        Assertions.assertThat(receivedBytes >= TestRetryLaterController.LARGE_BODY_BYTES
                || failureMessage != null).isTrue();
        if (failureMessage != null) {
            //it is a truncated stream, not a retry-later status: the refusal did not become answerable any more
            Assertions.assertThat(failureMessage).doesNotContain("503");
            Assertions.assertThat(failureMessage).doesNotContain("Service is currently initializing");
        }
    }

    private byte[] downloadBody(final String uri) throws IOException {
        try (InputStream in = URIs.connect(uri)
                .putBasicAuth(IntegrationWsProperties.SPRING_WEB_USER, IntegrationWsProperties.SPRING_WEB_PASSWORD)
                .downloadInputStream()) {
            return IOUtils.toByteArray(in);
        }
    }

    private String newUri(final String methodPath) {
        return IntegrationProperties.WEBSERVER_BIND_URI + "/spring-web" + PATH + methodPath;
    }

    /**
     * Throwables.DEBUG_STACK_TRACE_ENABLED is the operator's switch for getting the detail back that these reports
     * leave out by default: the 503 body must then carry the chain, and still no servlet error page.
     */
    @Test
    public void testDebugStackTraceEnabledAddsTheChainToTheBody() throws IOException {
        Throwables.setDebugStackTraceEnabled(true);
        try {
            final String body = assertRetryLaterStatus(newUri("/refusal"));
            Assertions.assertThat(body).contains("at de.invesdwin.");
            Assertions.assertThat(body).contains("RetryLaterPendingRuntimeException");
        } finally {
            Throwables.setDebugStackTraceEnabled(false);
        }
    }

    private String assertRetryLaterStatus(final String uri) throws IOException {
        try {
            URIs.connect(uri)
                    .putBasicAuth(IntegrationWsProperties.SPRING_WEB_USER, IntegrationWsProperties.SPRING_WEB_PASSWORD)
                    .downloadInputStream();
            Assertions.fail("the retryable refusal must not produce a success response: " + uri);
            return null;
        } catch (final IOException e) {
            //the http client surfaces any non-2xx as IOException carrying the status code and the response body; the
            //point is that it is a retry-later status WITHOUT the webserver servlet error page body (which previously
            //contained the whole servlet exception and stack of the RetryLater, embedded for HTTP 500 responses)
            Assertions.assertThat(e.getMessage()).contains("503");
            Assertions.assertThat(e.getMessage()).doesNotContain("ServletException");
            Assertions.assertThat(e.getMessage()).doesNotContain("HTTP ERROR");
            return e.getMessage();
        }
    }

}
