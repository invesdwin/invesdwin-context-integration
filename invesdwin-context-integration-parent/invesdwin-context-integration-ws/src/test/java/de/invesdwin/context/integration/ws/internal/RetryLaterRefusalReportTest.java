package de.invesdwin.context.integration.ws.internal;

import javax.annotation.concurrent.Immutable;

import org.junit.jupiter.api.Test;

import de.invesdwin.context.integration.ws.internal.RetryLaterRefusalReports.KeyReport;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.time.date.FTimeUnit;
import de.invesdwin.util.time.duration.Duration;

/**
 * A retry-later refusal has to be visible in the server log without becoming one line per request: the refusal path
 * used to log only at DEBUG, which is off on a running node, so a consumer stuck retrying for hours produced no
 * server-side record at all.
 * <p>
 * Red first: {@link RetryLaterRefusalReports} did not exist before this change.
 */
@Immutable
public class RetryLaterRefusalReportTest {

    private static final Duration INTERVAL = Duration.ONE_MINUTE;
    private static final long INTERVAL_NANOS = INTERVAL.longValue(FTimeUnit.NANOSECONDS);
    private static final long T0 = 1_000_000_000_000L;

    @Test
    public void theFirstRefusalForAKeyIsAlwaysReported() {
        Assertions.assertThat(new KeyReport().record(T0, INTERVAL))
                .as("an unseen refusal must not wait out the interval before it is visible")
                .isEqualTo(1L);
    }

    @Test
    public void refusalsInsideTheIntervalAreCountedInsteadOfReported() {
        final KeyReport report = new KeyReport();
        report.record(T0, INTERVAL);

        Assertions.assertThat(report.record(T0 + INTERVAL_NANOS / 2, INTERVAL))
                .as("the second refusal inside the interval gets no line")
                .isZero();
        Assertions.assertThat(report.getRefusalsSinceReport())
                .as("but it is counted, so the next line is not a sample")
                .isEqualTo(1L);
    }

    @Test
    public void aReportCoversEveryRefusalSinceThePreviousOne() {
        final KeyReport report = new KeyReport();
        Assertions.assertThat(report.record(T0, INTERVAL)).isEqualTo(1L);
        report.record(T0 + 1_000L, INTERVAL);
        report.record(T0 + 2_000L, INTERVAL);
        report.record(T0 + 3_000L, INTERVAL);

        //the interval is exclusive, so this is one nanosecond past it
        Assertions.assertThat(report.record(T0 + INTERVAL_NANOS + 1L, INTERVAL))
                .as("three suppressed plus this one")
                .isEqualTo(4L);
        Assertions.assertThat(report.getRefusalsSinceReport())
                .as("and the counter starts over, so no refusal is counted twice")
                .isZero();
        Assertions.assertThat(report.record(T0 + INTERVAL_NANOS + 1_000L, INTERVAL))
                .as("the next one inside the new interval is suppressed again")
                .isZero();
    }

    @Test
    public void eachKeyKeepsItsOwnIntervalAndItsOwnCount() {
        final KeyReport barsRoute = new KeyReport();
        final KeyReport ticksRoute = new KeyReport();

        Assertions.assertThat(barsRoute.record(T0, INTERVAL)).isEqualTo(1L);
        Assertions.assertThat(barsRoute.record(T0 + 1_000L, INTERVAL)).isZero();
        Assertions.assertThat(ticksRoute.record(T0 + 2_000L, INTERVAL))
                .as("a different (route, verdict) pair must not be silenced by the first one's interval")
                .isEqualTo(1L);
        Assertions.assertThat(barsRoute.getRefusalsSinceReport())
                .as("and the two counts never mix")
                .isEqualTo(1L);
    }

    @Test
    public void aStuckRefusalKeepsBeingReportedOncePerInterval() {
        final KeyReport report = new KeyReport();
        long reportedLines = 0L;
        long coveredRefusals = 0L;
        for (int i = 0; i < 600; i++) {
            //one refusal per second for ten minutes, the shape a client @Retry loop produces
            final long coveredByThisCall = report.record(T0 + i * 1_000_000_000L, INTERVAL);
            if (coveredByThisCall > 0L) {
                reportedLines++;
                coveredRefusals += coveredByThisCall;
            }
        }
        Assertions.assertThat(reportedLines)
                .as("ten minutes of a per-second storm is ten lines, not six hundred")
                .isEqualTo(10L);
        Assertions.assertThat(coveredRefusals)
                .as("the first line covers 1 and each later one covers its 61, the 50 refusals inside the open "
                        + "interval wait for the next line - the interval boundary is strict, so a refusal exactly "
                        + "one interval later is not yet due")
                .isEqualTo(550L);
    }

}
