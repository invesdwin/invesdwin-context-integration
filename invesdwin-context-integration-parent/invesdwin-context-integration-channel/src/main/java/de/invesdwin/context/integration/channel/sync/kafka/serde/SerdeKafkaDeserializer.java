package de.invesdwin.context.integration.channel.sync.kafka.serde;

import javax.annotation.concurrent.Immutable;

import org.apache.kafka.common.serialization.Deserializer;

import de.invesdwin.util.marshallers.serde.ISerde;

@Immutable
public class SerdeKafkaDeserializer<E> implements Deserializer<E> {

    private final ISerde<E> serde;

    public SerdeKafkaDeserializer(final ISerde<E> serde) {
        this.serde = serde;
    }

    @Override
    public E deserialize(final String topic, final byte[] data) {
        return serde.fromBytes(data);
    }

}
