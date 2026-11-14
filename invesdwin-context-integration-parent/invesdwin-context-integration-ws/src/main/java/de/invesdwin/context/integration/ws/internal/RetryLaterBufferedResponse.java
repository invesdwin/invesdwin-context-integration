package de.invesdwin.context.integration.ws.internal;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.util.math.Integers;
import de.invesdwin.util.streams.expandable.AccessibleByteArrayOutputStream;
import jakarta.servlet.ServletOutputStream;
import jakarta.servlet.WriteListener;
import jakarta.servlet.http.HttpServletResponse;
import jakarta.servlet.http.HttpServletResponseWrapper;

/**
 * Buffers a spring-web response body so that a retryable refusal can still be answered cleanly.
 *
 * A streaming handler that iterates a demand into {@code response.getOutputStream()} commits the response before
 * it knows whether the demand will answer: the bean writer starts the format block on construction and the
 * try-with-resources closes it on the way out of a failed write, and that close flushes. From then on
 * {@link RetryLaterFilter} cannot answer the refusal any more (it deliberately leaves a committed response alone),
 * so the exception reaches the webserver, which logs a WARN with a full stack per request while the client receives
 * a truncated body without a message - measured on ptstaging 2026-11-12: 230 such records in one 6-minute run, 229
 * of them inside 61 seconds on a single financialdatalive/getTicks range.
 *
 * This wrapper keeps the body out of the container until the chain has finished: while nothing has been released,
 * writes go into a bounded buffer and the container stream is never touched. {@link #flushBufferedBody()} hands the
 * buffer over on success; if the chain fails with a retryable refusal while the body is still buffered
 * ({@link #isBodyInFlight()} false), the buffer is discarded and the filter answers as it does for any refusal
 * before a response has been opened.
 *
 * The buffer is bounded (IntegrationWsProperties.SPRING_WEB_RETRY_LATER_BUFFER_BYTES): a body larger than that is a
 * real stream and must keep flowing, so the wrapper releases it to the container and becomes pass-through from then
 * on. Such a response cannot be converted any more - that is the honest limit of the guard, not an oversight. A size
 * of zero disables buffering and keeps the previous behaviour.
 */
@NotThreadSafe
public final class RetryLaterBufferedResponse extends HttpServletResponseWrapper {

    private final int maxBufferBytes;
    private final AccessibleByteArrayOutputStream buffer;
    private boolean bodyInFlight;
    private ServletOutputStream containerOut;
    private DeferringServletOutputStream deferringOut;
    private PrintWriter writer;

    public RetryLaterBufferedResponse(final HttpServletResponse response, final int maxBufferBytes) {
        super(response);
        this.maxBufferBytes = Integers.max(0, maxBufferBytes);
        this.buffer = new AccessibleByteArrayOutputStream(
                this.maxBufferBytes > 0 ? Integers.min(this.maxBufferBytes, 8 * 1024) : 0);
    }

    @Override
    public ServletOutputStream getOutputStream() throws IOException {
        if (isPassThrough()) {
            return super.getOutputStream();
        }
        if (deferringOut == null) {
            deferringOut = new DeferringServletOutputStream();
        }
        return deferringOut;
    }

    @Override
    public PrintWriter getWriter() throws IOException {
        if (isPassThrough()) {
            return super.getWriter();
        }
        if (writer == null) {
            final String characterEncoding = getCharacterEncoding();
            final Charset charset = characterEncoding != null ? Charset.forName(characterEncoding)
                    : StandardCharsets.UTF_8;
            writer = new PrintWriter(new OutputStreamWriter(getOutputStream(), charset));
        }
        return writer;
    }

    @Override
    public void reset() {
        buffer.reset();
        bodyInFlight = false;
        containerOut = null;
        deferringOut = null;
        writer = null;
        super.reset();
    }

    @Override
    public void sendError(final int status, final String message) throws IOException {
        //the container writes the error page itself: give up the buffered body instead of ordering it first
        buffer.reset();
        bodyInFlight = true;
        super.sendError(status, message);
    }

    @Override
    public void sendError(final int status) throws IOException {
        buffer.reset();
        bodyInFlight = true;
        super.sendError(status);
    }

    /**
     * True once bytes have reached the container, so the response can no longer be replaced by a retry-later answer.
     */
    public boolean isBodyInFlight() {
        return bodyInFlight || super.isCommitted();
    }

    /**
     * The chain produced its answer: hand the buffered body to the container (it is already there for a body that
     * overflowed the buffer).
     */
    public void flushBufferedBody() throws IOException {
        if (writer != null) {
            writer.flush();
        }
        if (!bodyInFlight) {
            if (buffer.size() > 0) {
                containerOut = super.getOutputStream();
                containerOut.write(buffer.array(), 0, buffer.size());
                buffer.reset();
            }
            bodyInFlight = true;
        }
        if (containerOut != null) {
            if (deferringOut != null && deferringOut.closed) {
                //the handler closed its stream: mirror that for the container, which also flushes
                containerOut.close();
            } else {
                containerOut.flush();
            }
        }
    }

    /**
     * The chain failed with something the filter will answer: drop what was buffered and never let it reach the
     * client.
     */
    public void discardBufferedBody() {
        buffer.reset();
    }

    private boolean isPassThrough() {
        return maxBufferBytes == 0 || bodyInFlight;
    }

    private void releaseBufferToContainer() throws IOException {
        containerOut = super.getOutputStream();
        containerOut.write(buffer.array(), 0, buffer.size());
        buffer.reset();
        bodyInFlight = true;
    }

    @NotThreadSafe
    private final class DeferringServletOutputStream extends ServletOutputStream {

        private boolean closed;

        @Override
        public void write(final int b) throws IOException {
            checkOpen();
            if (bodyInFlight) {
                containerOut.write(b);
                return;
            }
            if (buffer.size() + 1 > maxBufferBytes) {
                releaseBufferToContainer();
                containerOut.write(b);
                return;
            }
            buffer.write(b);
        }

        @Override
        public void write(final byte[] b, final int off, final int len) throws IOException {
            checkOpen();
            if (bodyInFlight) {
                containerOut.write(b, off, len);
                return;
            }
            if (buffer.size() + len > maxBufferBytes) {
                releaseBufferToContainer();
                containerOut.write(b, off, len);
                return;
            }
            buffer.write(b, off, len);
        }

        @Override
        public void flush() throws IOException {
            //while deferring nothing has reached the container, so there is nothing to flush
            if (bodyInFlight && containerOut != null) {
                containerOut.flush();
            }
        }

        @Override
        public void close() throws IOException {
            closed = true;
            //the container stream is NOT closed here: the chain outcome decides whether the body goes out at all
            if (bodyInFlight && containerOut != null) {
                containerOut.close();
            }
        }

        @Override
        public boolean isReady() {
            return true;
        }

        @Override
        public void setWriteListener(final WriteListener writeListener) {}

        private void checkOpen() throws IOException {
            if (closed) {
                throw new IOException("stream already closed: " + getClass().getSimpleName());
            }
        }
    }

}
