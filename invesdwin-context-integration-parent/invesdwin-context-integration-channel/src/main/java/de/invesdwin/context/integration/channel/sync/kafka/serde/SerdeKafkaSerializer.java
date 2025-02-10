package de.invesdwin.context.integration.channel.sync.kafka.serde;

import javax.annotation.concurrent.Immutable;

import org.apache.kafka.common.serialization.Serializer;

import de.invesdwin.util.marshallers.serde.ISerde;

@Immutable
public class SerdeKafkaSerializer<E> implements Serializer<E> {

    private final ISerde<E> serde;

    public SerdeKafkaSerializer(final ISerde<E> serde) {
        this.serde = serde;
    }

    @Override
    public byte[] serialize(final String topic, final E data) {
        return serde.toBytes(data);
    }

}
