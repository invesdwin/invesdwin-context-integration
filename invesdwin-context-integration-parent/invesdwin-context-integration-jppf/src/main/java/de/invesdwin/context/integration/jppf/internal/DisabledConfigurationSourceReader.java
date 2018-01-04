package de.invesdwin.context.integration.jppf.internal;

import java.io.IOException;
import java.io.Reader;

import javax.annotation.concurrent.NotThreadSafe;

import org.jppf.utils.JPPFConfiguration.ConfigurationSourceReader;

@NotThreadSafe
public class DisabledConfigurationSourceReader implements ConfigurationSourceReader {

    @Override
    public Reader getPropertyReader() throws IOException {
        return null;
    }

}
