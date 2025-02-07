package de.invesdwin.context.integration.channel.report;

import java.io.Closeable;

import de.invesdwin.util.collections.iterable.ICloseableIterable;
import de.invesdwin.util.time.date.FDate;
import de.invesdwin.util.time.date.IFDateProvider;

public interface ILatencyReport extends Closeable {

    ICloseableIterable<? extends IFDateProvider> newRequestMessages();

    IFDateProvider newResponseMessage(FDate request);

    IFDateProvider newArrivalTimestamp();

    void measureLatency(FDate message, FDate arrivalTimestamp);

    default void measureLatency(final FDate message) {
        measureLatency(message, newArrivalTimestamp().asFDate());
    }

    boolean isMeasuringLatency();

    @Override
    void close();

    void validateResponse(FDate request, FDate response);

}
