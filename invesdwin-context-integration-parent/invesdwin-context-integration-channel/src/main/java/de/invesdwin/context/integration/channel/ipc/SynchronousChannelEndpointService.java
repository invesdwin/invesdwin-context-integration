package de.invesdwin.context.integration.channel.ipc;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodHandles.Lookup;
import java.lang.reflect.Method;

import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.norva.beanpath.spi.ABeanPathProcessor;
import de.invesdwin.util.collections.Arrays;
import de.invesdwin.util.error.Throwables;
import de.invesdwin.util.error.UnknownArgumentException;
import de.invesdwin.util.lang.Objects;
import de.invesdwin.util.lang.reflection.Reflections;
import de.invesdwin.util.marshallers.serde.ISerde;
import de.invesdwin.util.math.Shorts;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import it.unimi.dsi.fastutil.shorts.Short2ObjectMap;
import it.unimi.dsi.fastutil.shorts.Short2ObjectOpenHashMap;

@ThreadSafe
public final class SynchronousChannelEndpointService {

    public static final int TYPE_INDEX = 0;
    public static final int TYPE_SIZE = Integer.BYTES;

    public static final int METHOD_INDEX = TYPE_INDEX + TYPE_SIZE;
    public static final int METHOD_SIZE = Short.BYTES;

    public static final int ARGSSIZE_INDEX = METHOD_INDEX + METHOD_SIZE;
    public static final int ARGSSIZE_SIZE = Integer.BYTES;

    public static final int ARGS_INDEX = ARGSSIZE_INDEX + ARGSSIZE_SIZE;

    private final Class<?> interfaceType;
    private final int interfaceTypeId;
    private final Object implementation;
    private final ISerde<Object> genericSerde;
    private final Short2ObjectMap<MethodHandle> index_method;

    private SynchronousChannelEndpointService(final Class<?> interfaceType, final Object implementation,
            final ISerde<Object> genericSerde) {
        this.interfaceType = interfaceType;
        this.interfaceTypeId = newInterfaceTypeId(interfaceType);
        this.implementation = implementation;
        this.genericSerde = genericSerde;
        final Method[] methods = Reflections.getUniqueDeclaredMethods(interfaceType);
        this.index_method = new Short2ObjectOpenHashMap<>(methods.length);
        Arrays.sort(methods, Reflections.METHOD_COMPARATOR);
        int ignoredMethods = 0;
        final Lookup lookup = MethodHandles.lookup();
        for (int i = 0; i < methods.length; i++) {
            final Method method = methods[i];
            final int indexOf = Arrays.indexOf(ABeanPathProcessor.ELEMENT_NAME_BLACKLIST, method.getName());
            if (indexOf < 0) {
                final short methodIndex = Shorts.checkedCast(i - ignoredMethods);
                try {
                    index_method.put(methodIndex, lookup.unreflect(method));
                } catch (final IllegalAccessException e) {
                    throw new RuntimeException(e);
                }
            } else {
                ignoredMethods++;
            }
        }
    }

    public static int newInterfaceTypeId(final Class<?> interfaceType) {
        return interfaceType.getName().hashCode();
    }

    public int getInterfaceTypeId() {
        return interfaceTypeId;
    }

    public Class<?> getInterfaceType() {
        return interfaceType;
    }

    public int invoke(final IByteBuffer request, final IByteBuffer response) {
        final short methodIndex = request.getShort(METHOD_INDEX);
        final int argsSize = request.getInt(ARGSSIZE_INDEX);
        final Object[] args = (Object[]) genericSerde.fromBuffer(request.slice(ARGS_INDEX, argsSize));
        final Object result = invoke(methodIndex, args);
        final int responseSize = genericSerde.toBuffer(response, result);
        return responseSize;
    }

    private Object invoke(final short methodIndex, final Object... args) {
        final MethodHandle method = index_method.get(methodIndex);
        if (method == null) {
            throw UnknownArgumentException.newInstance(Short.class, methodIndex);
        }
        try {
            final Object result = method.invoke(implementation, args);
            return result;
        } catch (final Throwable e) {
            throw Throwables.propagate(e);
        }
    }

    public static <T> SynchronousChannelEndpointService newInstance(final Class<? super T> interfaceType,
            final T implementation, final ISerde<Object> genericSerde) {
        return new SynchronousChannelEndpointService(interfaceType, implementation, genericSerde);
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("interfaceTypeId", interfaceTypeId)
                .add("interfaceType", interfaceType.getName())
                .toString();
    }

}
