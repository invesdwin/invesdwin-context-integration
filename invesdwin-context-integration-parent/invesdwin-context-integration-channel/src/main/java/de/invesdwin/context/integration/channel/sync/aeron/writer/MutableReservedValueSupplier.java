package de.invesdwin.context.integration.channel.sync.aeron.writer;

import javax.annotation.concurrent.NotThreadSafe;

import org.agrona.DirectBuffer;

import io.aeron.ReservedValueSupplier;

@NotThreadSafe
public class MutableReservedValueSupplier implements ReservedValueSupplier {

    private long reservedValue;

    public void setReservedValue(final long reservedValue) {
        this.reservedValue = reservedValue;
    }

    public long getReservedValue() {
        return reservedValue;
    }

    @Override
    public long get(final DirectBuffer termBuffer, final int termOffset, final int frameLength) {
        return reservedValue;
    }

}
