package de.invesdwin.context.integration.channel.sync.kryonet.connection;

import java.io.Closeable;
import java.io.IOException;

import com.esotericsoftware.kryonet.Listener;

import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

public interface IKryonetConnection extends Closeable {

    void send(IByteBufferProvider message);

    void addListener(Listener listener);

    void update() throws IOException;

}
