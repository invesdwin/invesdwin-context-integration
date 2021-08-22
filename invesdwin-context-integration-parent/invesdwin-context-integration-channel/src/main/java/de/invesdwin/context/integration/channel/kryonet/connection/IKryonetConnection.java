package de.invesdwin.context.integration.channel.kryonet.connection;

import java.io.Closeable;
import java.io.IOException;

import com.esotericsoftware.kryonet.Listener;

import de.invesdwin.context.integration.channel.message.ISynchronousMessage;

public interface IKryonetConnection extends Closeable {

    void send(ISynchronousMessage<byte[]> message);

    void addListener(Listener listener);

    void update() throws IOException;

}
