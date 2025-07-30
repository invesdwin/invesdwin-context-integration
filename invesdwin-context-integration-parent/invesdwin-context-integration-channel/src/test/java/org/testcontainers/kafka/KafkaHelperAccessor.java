package org.testcontainers.kafka;

import java.util.Collection;
import java.util.Set;
import java.util.function.Supplier;

import javax.annotation.concurrent.Immutable;

@Immutable
public final class KafkaHelperAccessor {

    private KafkaHelperAccessor() {}

    public static Collection<? extends String> resolveAdvertisedListeners(
            final Set<Supplier<String>> thisAdvertisedListeners) {
        return KafkaHelper.resolveAdvertisedListeners(thisAdvertisedListeners);
    }

}
