package de.invesdwin.context.integration.channel.report;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.time.date.FDate;
import de.invesdwin.util.time.duration.Duration;

/**
 * This report does not incur latency measurement overhead, it just makes sure messages are in correct order.
 */
@NotThreadSafe
public class ValidatingLatencyReport extends DisabledLatencyReport {

    @Override
    public void validateResponse(final FDate request, final FDate response) {
        Assertions.checkEquals(request.addPicoseconds(1), response);
    }

    @Override
    public void validateOrder(final FDate prevValue, final FDate nextValue) {
        if (prevValue != null && !prevValue.addPicoseconds(1).equalsNotNullSafe(nextValue)) {
            final Duration distance = new Duration(prevValue, nextValue);
            throw new IllegalStateException("nextValue [" + nextValue + "] should be 1ps after prevValue [" + prevValue
                    + "] but it is " + distance + " after");
        }
    }

}
