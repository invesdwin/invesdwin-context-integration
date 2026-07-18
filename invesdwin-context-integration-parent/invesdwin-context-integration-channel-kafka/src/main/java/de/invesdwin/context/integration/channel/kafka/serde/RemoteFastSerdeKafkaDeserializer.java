package de.invesdwin.context.integration.channel.kafka.serde;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.util.marshallers.serde.RemoteFastSerializingSerde;

@Immutable
public class RemoteFastSerdeKafkaDeserializer<E> extends SerdeKafkaDeserializer<E> {

    public RemoteFastSerdeKafkaDeserializer() {
        super(new RemoteFastSerializingSerde<E>(true));
    }

}
