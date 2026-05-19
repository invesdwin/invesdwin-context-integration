package de.invesdwin.context.integration.channel.sync.timeseriesdb;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.util.time.date.FDate;
import de.invesdwin.util.time.date.FDates;

@Immutable
public enum TimeSeriesDBSynchronousChannelIndexMode {
    TIME {
        @Override
        public FDate nextIndex(final FDate lastIndex) {
            final FDate now = FDate.now();
            if (lastIndex.isBeforeNotNullSafe(now)) {
                return now;
            } else {
                //increment milliseconds for multiple messages at the same timestamp
                return lastIndex.addPicoseconds(1);
            }
        }
    },
    INDEX {
        @Override
        public FDate nextIndex(final FDate lastIndex) {
            return lastIndex.addMilliseconds(1);
        }
    };

    public abstract FDate nextIndex(FDate lastIndex);

    public FDate initialIndex() {
        return FDates.MIN_DATE;
    }
}
