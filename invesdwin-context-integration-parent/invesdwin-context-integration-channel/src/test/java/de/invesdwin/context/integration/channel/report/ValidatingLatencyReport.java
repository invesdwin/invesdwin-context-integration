package de.invesdwin.context.integration.channel.report;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.time.date.FDate;

/**
 * This report does not incur latency measurement overhead, it just makes sure messages are in correct order.
 */
@NotThreadSafe
public class ValidatingLatencyReport extends DisabledLatencyReport {

    @Override
    public void validateResponse(final FDate request, final FDate response) {
        Assertions.checkEquals(request.addMilliseconds(1), response);
    }

    @Override
    public void validateOrder(final FDate prevValue, final FDate nextValue) {
        if (prevValue != null && !prevValue.isBeforeNotNullSafe(nextValue)) {
            Assertions.assertThat(prevValue).isBefore(nextValue);
        }
    }

}
