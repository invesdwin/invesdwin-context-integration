package de.invesdwin.context.integration.channel.sync.kafka.serde;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.util.marshallers.serde.RemoteFastSerializingSerde;

@Immutable
public class RemoteFastSerdeKafkaSerializer<E> extends SerdeKafkaSerializer<E> {

    public RemoteFastSerdeKafkaSerializer() {
        super(new RemoteFastSerializingSerde<E>(true));
    }

}
