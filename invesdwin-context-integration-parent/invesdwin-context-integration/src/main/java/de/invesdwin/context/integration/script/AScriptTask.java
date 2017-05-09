package de.invesdwin.context.integration.script;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.commons.io.IOUtils;
import org.springframework.core.io.Resource;

@NotThreadSafe
public abstract class AScriptTask<V, R> {

    public abstract Resource getScriptResource();

    public String getScriptResourceAsString() {
        try (InputStream in = getScriptResource().getInputStream()) {
            return IOUtils.toString(in, StandardCharsets.UTF_8);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    public Reader getScriptResourceAsReader() {
        try {
            return new InputStreamReader(getScriptResource().getInputStream());
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    public abstract void populateInputs(IScriptTaskInputs inputs);

    public abstract V extractResults(IScriptTaskResults results);

    public abstract V run(final R runner);

    /**
     * Runs this script task with the provided/preferred/default runner or throws an exception when none can be
     * identified uniquely
     */
    public abstract V run();

}
