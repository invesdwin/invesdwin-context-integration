package de.invesdwin.context.integration.channel.sync.pipe.unsafe;

import javax.annotation.concurrent.Immutable;

@SuppressWarnings("restriction")
@Immutable
public final class IOStatusAccessor {

    public static final int EOF = sun.nio.ch.IOStatus.EOF; // End of file
    public static final int UNAVAILABLE = sun.nio.ch.IOStatus.UNAVAILABLE; // Nothing available (non-blocking)
    public static final int INTERRUPTED = sun.nio.ch.IOStatus.INTERRUPTED; // System call interrupted
    public static final int UNSUPPORTED = sun.nio.ch.IOStatus.UNSUPPORTED; // Operation not supported
    public static final int THROWN = sun.nio.ch.IOStatus.THROWN; // Exception thrown in JNI code
    public static final int UNSUPPORTED_CASE = sun.nio.ch.IOStatus.UNSUPPORTED_CASE; // This case not supported

    private IOStatusAccessor() {}

    public static int normalize(final int n) {
        return sun.nio.ch.IOStatus.normalize(n);
    }

    public static long normalize(final long n) {
        return sun.nio.ch.IOStatus.normalize(n);
    }

}
