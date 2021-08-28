package de.invesdwin.context.integration.channel.kryonet.connection;

import java.io.Closeable;
import java.io.IOException;

import com.esotericsoftware.kryonet.Listener;

import de.invesdwin.util.lang.buffer.IByteBuffer;

public interface IKryonetConnection extends Closeable {

    void send(IByteBuffer message);

    void addListener(Listener listener);

    void update() throws IOException;

}
