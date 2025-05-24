package de.invesdwin.context.integration.channel.sync.aeron.writer;

import javax.annotation.concurrent.Immutable;

import org.agrona.DirectBuffer;

import io.aeron.ReservedValueSupplier;

@Immutable
public class DisabledReservedValueSupplier implements ReservedValueSupplier {

    public static final long DISABLED_RESERVED_VALUE = -1L;
    public static final DisabledReservedValueSupplier INSTANCE = new DisabledReservedValueSupplier();

    @Override
    public long get(final DirectBuffer termBuffer, final int termOffset, final int frameLength) {
        return DISABLED_RESERVED_VALUE;
    }

}
