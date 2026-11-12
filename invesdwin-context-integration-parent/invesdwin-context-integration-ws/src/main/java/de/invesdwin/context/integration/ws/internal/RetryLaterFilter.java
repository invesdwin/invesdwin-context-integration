package de.invesdwin.context.integration.ws.internal;

import java.io.IOException;

import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.integration.retry.IRetryLaterException;
import de.invesdwin.context.integration.ws.IntegrationWsProperties;
import de.invesdwin.context.log.Log;
import de.invesdwin.util.error.Throwables;
import jakarta.servlet.Filter;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.ServletRequest;
import jakarta.servlet.ServletResponse;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

/**
 * Requests arriving while a service is loading or restarting are answered by the handlers with the retryable
 * family (the IRetryLaterException subtypes): the @RequestMapping handler methods run retry-disabled via
 * RetryAspect.retryDisabledRequestMapping, so a RetryLater leaves the handler on every request instead of
 * looping. Spring has no resolver for that family, so without this filter the exception travels up through the
 * servlet into the webserver, which logs the full servlet exception stack per request (HttpChannel WARN) while
 * serving its HTML error page - error spamming for what is a normal 'retry later' state.
 *
 * This filter maps any spring-web request failure whose cause chain carries a retryable to a clean 503 with the
 * short retry message and no stack, so the webserver never sees the exception. With
 * Throwables.DEBUG_STACK_TRACE_ENABLED the full chain is appended to the body, which is how the operator gets the
 * server-side detail back without a rebuild. The cause chain must be searched
 * and not just the top exception type, because the retryable often arrives wrapped - e.g. Futures.get re-throws
 * a joined async update future's cause as a plain RuntimeException, which loses the subtype. The
 * IRetryLaterException marker alone does not carry a message, so the found marker is used as the Throwable it
 * must be.
 *
 * The catch is Exception and not only the checked ServletException | IOException: every IRetryLaterException
 * implementor is unchecked (RetryLaterRuntimeException and its subtypes), and spring-web rethrows an unchecked
 * handler failure unwrapped, so a checked-only catch would never see the family it exists for.
 *
 * The response is wrapped in {@link RetryLaterBufferedResponse}, so "streamed data started" no longer means
 * "committed": a refusal that surfaces while a demand is being written into the response is still answerable as long
 * as the buffered body has not exceeded IntegrationWsProperties.SPRING_WEB_RETRY_LATER_BUFFER_BYTES. A body larger
 * than that is a real stream and goes to the container while the handler is still writing, and a failure after that
 * point cannot be rewritten without corrupting the body, so it keeps the old path.
 *
 * The consumer side is unaffected: the http client surfaces any non-2xx as an IOException, which is in the
 * allowed causes of the retry policies, so the calling @Retry aspects back off exactly as they did with the
 * webserver error page before.
 */
@ThreadSafe
public class RetryLaterFilter implements Filter {

    private static final Log LOG = new Log(RetryLaterFilter.class);

    @Override
    public void doFilter(final ServletRequest request, final ServletResponse response, final FilterChain chain)
            throws IOException, ServletException {
        if (!(request instanceof HttpServletRequest) || !(response instanceof HttpServletResponse)) {
            chain.doFilter(request, response);
            return;
        }
        final HttpServletRequest httpRequest = (HttpServletRequest) request;
        final HttpServletResponse httpResponse = (HttpServletResponse) response;
        //the body is buffered so that a refusal surfacing while a demand is streamed into the response can still be
        //answered here; see RetryLaterBufferedResponse for why the alternative is a webserver WARN per request
        final RetryLaterBufferedResponse deferred = new RetryLaterBufferedResponse(httpResponse,
                IntegrationWsProperties.SPRING_WEB_RETRY_LATER_BUFFER_BYTES);
        try {
            chain.doFilter(request, deferred);
            deferred.flushBufferedBody();
        } catch (final Exception e) {
            final IRetryLaterException marker = Throwables.getCauseByType(e, IRetryLaterException.class);
            if (marker == null) {
                throw e;
            }
            //every IRetryLaterException implementor is a Throwable
            final Throwable retry = (Throwable) marker;
            if (deferred.isBodyInFlight()) {
                //streaming already started, a 503 cannot replace the partial body anymore
                throw e;
            }
            deferred.discardBufferedBody();
            if (LOG.isDebugEnabled()) {
                LOG.debug("Answering with retry-later for [%s]: %s", httpRequest.getRequestURI(), retry.getMessage());
            }
            httpResponse.reset();
            httpResponse.setStatus(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
            httpResponse.setContentType("text/plain");
            httpResponse.setCharacterEncoding("UTF-8");
            //the body is short by design (the http client turns a non-2xx into an IOException carrying it). It names the
            //verdict class plus its cause chain; with Throwables.DEBUG_STACK_TRACE_ENABLED the full stack of the verdict
            //comes along too, which is how the operator gets the server-side detail back without a rebuild.
            httpResponse.getWriter().write(Throwables.getMessagesAndStackTraceIfDebugEnabled(retry));
        }
    }

}
