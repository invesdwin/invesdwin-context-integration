package de.invesdwin.context.integration.channel.kryonet.connection;

import java.io.Closeable;
import java.io.IOException;

import com.esotericsoftware.kryonet.Listener;

import de.invesdwin.context.integration.channel.command.ISynchronousCommand;

public interface IKryonetConnection extends Closeable {

    void send(ISynchronousCommand<byte[]> message);

    void addListener(Listener listener);

    void update() throws IOException;

}
