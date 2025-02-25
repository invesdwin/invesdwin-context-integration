package de.invesdwin.context.integration.channel.report;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.util.collections.iterable.ICloseableIterable;
import de.invesdwin.util.collections.iterable.ICloseableIterator;
import de.invesdwin.util.time.date.FDate;
import de.invesdwin.util.time.date.FDates;
import de.invesdwin.util.time.date.IFDateProvider;

/**
 * This report does not incur latency measurement overhead, it just makes sure messages are correct.
 */
@NotThreadSafe
public class DisabledLatencyReport implements ILatencyReport {

    private static final IFDateProvider ARRIVAL_TIMESTAMP_PROVIDER = new IFDateProvider() {
        @Override
        public FDate asFDate() {
            return null;
        }
    };

    @Override
    public ICloseableIterable<? extends IFDateProvider> newRequestMessages() {
        return new ICloseableIterable<IFDateProvider>() {
            @Override
            public ICloseableIterator<IFDateProvider> iterator() {
                return new ICloseableIterator<IFDateProvider>() {

                    private FDate cur = FDates.MIN_DATE;

                    @Override
                    public IFDateProvider next() {
                        cur = cur.addMilliseconds(1);
                        return cur;
                    }

                    @Override
                    public boolean hasNext() {
                        return true;
                    }

                    @Override
                    public void close() {}
                };
            }
        };
    }

    @Override
    public FDate newResponseMessage(final FDate request) {
        return request.addMilliseconds(1);
    }

    @Override
    public boolean isMeasuringLatency() {
        return false;
    }

    @Override
    public IFDateProvider newArrivalTimestamp() {
        return ARRIVAL_TIMESTAMP_PROVIDER;
    }

    @Override
    public void measureLatency(final int index, final FDate message) {
        //noop
    }

    @Override
    public void measureLatency(final int index, final FDate message, final FDate arrivalTimestamp) {
        //noop
    }

    @Override
    public void validateResponse(final FDate request, final FDate response) {
        //noop
    }

    @Override
    public void validateOrder(final FDate prevValue, final FDate nextValue) {
        //noop
    }

    @Override
    public void close() {}

}
