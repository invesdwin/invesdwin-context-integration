package de.invesdwin.context.integration.ws.internal;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.log.Log;
import de.invesdwin.util.collections.factory.ILockCollectionFactory;
import de.invesdwin.util.time.date.millis.FDateNanos;
import de.invesdwin.util.time.duration.Duration;

/**
 * Reports that request threads are being answered retry-later, once per (route, verdict) per interval, with the
 * number of refusals the line covers.
 *
 * <p>
 * The refusal path was otherwise silent at production log levels: {@link RetryLaterFilter} answers a whole storm of
 * them with a 503 whose body names the verdict, and logged the site only at DEBUG, which is off on a running node. A
 * consumer stuck retrying for hours therefore produced no server-side record of what refused it, while the request
 * threads, the update locks and the response bodies all looked healthy.
 *
 * <p>
 * A line per request is not an option - that is what the filter exists to avoid - so the count is what makes the
 * report more than a sample. The key is route plus verdict class, so a stuck subsystem on one route cannot hide a
 * different refusal on another, and the two are separately countable.
 */
@ThreadSafe
final class RetryLaterRefusalReports {

    private static final Log LOG = new Log(RetryLaterFilter.class);

    private static final Duration REPORT_INTERVAL = Duration.ONE_MINUTE;

    private static final Map<String, KeyReport> REPORTS = ILockCollectionFactory.getInstance(true).newConcurrentMap();

    private RetryLaterRefusalReports() {
        //static entry point
    }

    static void record(final String requestUri, final Throwable retry) {
        final String key = requestUri + '|' + retry.getClass().getName();
        final long refusals = REPORTS.computeIfAbsent(key, k -> new KeyReport())
                .record(FDateNanos.elapsedNanos(), REPORT_INTERVAL);
        if (refusals > 0L) {
            LOG.warn("Answered [%s] request thread(s) with retry-later for [%s] via [%s]: %s", refusals, requestUri,
                    retry.getClass().getSimpleName(), retry.getMessage());
        }
    }

    /**
     * The report state of one (route, verdict) pair. Time is always passed in, so the interval logic is testable
     * without sleeping.
     */
    @ThreadSafe
    static final class KeyReport {

        private static final long NEVER_REPORTED = -1L;

        private final AtomicLong lastReportNanos = new AtomicLong(NEVER_REPORTED);
        private final AtomicLong refusalsSinceReport = new AtomicLong();

        /**
         * @return how many refusals this line covers, including the caller's own, or 0 when the caller's refusal is
         *         inside the interval of a refusal already reported (or claimed by another thread). The interval is
         *         exclusive: a refusal exactly one interval later is not yet due.
         */
        long record(final long nowNanos, final Duration reportInterval) {
            final long previousReportNanos = lastReportNanos.get();
            final boolean due = previousReportNanos == NEVER_REPORTED
                    || reportInterval.isLessThanNanos(nowNanos - previousReportNanos);
            if (!due || !lastReportNanos.compareAndSet(previousReportNanos, nowNanos)) {
                refusalsSinceReport.incrementAndGet();
                return 0L;
            }
            //the +1 is the caller's own refusal, which is never in the counter
            return refusalsSinceReport.getAndSet(0L) + 1L;
        }

        long getRefusalsSinceReport() {
            return refusalsSinceReport.get();
        }

    }

}
