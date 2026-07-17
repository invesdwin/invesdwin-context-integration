package de.invesdwin.context.integration.channel.sync.axon.serde;

import javax.annotation.concurrent.Immutable;

import org.axonframework.serialization.Converter;
import org.axonframework.serialization.SerializedObject;
import org.axonframework.serialization.SerializedType;
import org.axonframework.serialization.Serializer;
import org.axonframework.serialization.SimpleSerializedObject;
import org.axonframework.serialization.SimpleSerializedType;
import org.axonframework.serialization.UnknownSerializedType;

/**
 * Avoids some unnecessary overhead for plain byte arrays in the channel.
 */
@SuppressWarnings({ "rawtypes", "unchecked" })
@Immutable
public final class ByteArrayAxonSerializer implements Serializer {

    public static final ByteArrayAxonSerializer INSTANCE = new ByteArrayAxonSerializer();
    private static final SerializedType SERIALIZED_TYPE = new SimpleSerializedType(byte[].class.getSimpleName(), null);

    private ByteArrayAxonSerializer() {}

    @Override
    public <T> SerializedObject<T> serialize(final Object instance, final Class<T> expectedType) {
        return new SimpleSerializedObject(instance, expectedType, SERIALIZED_TYPE);
    }

    @Override
    public <T> boolean canSerializeTo(final Class<T> expectedRepresentation) {
        return expectedRepresentation.isAssignableFrom(byte[].class);
    }

    @Override
    public <S, T> T deserialize(final SerializedObject<S> serializedObject) {
        return (T) serializedObject.getData();
    }

    @Override
    public Class classForType(final SerializedType type) {
        if (SERIALIZED_TYPE.equals(type)) {
            return byte[].class;
        }
        if (SerializedType.emptyType().equals(type)) {
            return Void.class;
        }
        try {
            return Class.forName(type.getName());
        } catch (final ClassNotFoundException e) {
            return UnknownSerializedType.class;
        }
    }

    @Override
    public SerializedType typeForClass(final Class type) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Converter getConverter() {
        throw new UnsupportedOperationException();
    }

}
