package de.invesdwin.context.integration.channel.sync.kryonet.connection;

import java.io.Closeable;
import java.io.IOException;

import com.esotericsoftware.kryonet.Listener;

import de.invesdwin.util.streams.buffer.bytes.IByteBufferWriter;

public interface IKryonetConnection extends Closeable {

    void send(IByteBufferWriter message);

    void addListener(Listener listener);

    void update() throws IOException;

}
