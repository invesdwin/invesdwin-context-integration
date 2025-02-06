package de.invesdwin.context.integration.channel.sync.timeseriesdb;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.util.time.date.FDates;

@Immutable
public enum TimeSeriesDBSynchronousChannelIndexMode {
    TIME {
        @Override
        public long nextIndex(final long lastIndex) {
            final long nowMillis = System.currentTimeMillis();
            if (lastIndex < nowMillis) {
                return nowMillis;
            } else {
                //increment milliseconds for multiple messages at the same timestamp
                return lastIndex + 1;
            }
        }
    },
    INDEX {
        @Override
        public long nextIndex(final long lastIndex) {
            return lastIndex + 1;
        }
    };

    public abstract long nextIndex(long lastIndex);

    public long initialIndex() {
        return FDates.MIN_DATE.millisValue();
    }
}
