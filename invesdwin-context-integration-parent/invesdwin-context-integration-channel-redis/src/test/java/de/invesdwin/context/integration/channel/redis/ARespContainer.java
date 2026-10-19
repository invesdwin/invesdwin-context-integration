package de.invesdwin.context.integration.channel.redis;

import java.util.concurrent.TimeUnit;

import javax.annotation.concurrent.NotThreadSafe;

import org.rnorth.ducttape.unreliables.Unreliables;
import org.testcontainers.containers.ContainerLaunchException;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.AbstractWaitStrategy;
import org.testcontainers.containers.wait.strategy.WaitStrategy;
import org.testcontainers.utility.DockerImageName;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisException;

@NotThreadSafe
public abstract class ARespContainer<C extends ARespContainer<C>> extends GenericContainer<C> {
    protected static final int RESP_PORT = 6379;

    protected ARespContainer(final DockerImageName dockerImageName) {
        super(dockerImageName);
    }

    @Override
    protected void configure() {
        withNetworkMode("host");
        withCommand(newCommand(getRespPort()));
        waitingFor(newWaitStrategy());
    }

    protected abstract String[] newCommand(int port);

    protected WaitStrategy newWaitStrategy() {
        return new RespPingWaitStrategy(getRespHost(), getRespPort());
    }

    public String getRespHost() {
        return "127.0.0.1";
    }

    public int getRespPort() {
        return RESP_PORT;
    }
}

@NotThreadSafe
final class RespPingWaitStrategy extends AbstractWaitStrategy {
    private final String host;
    private final int port;

    RespPingWaitStrategy(final String host, final int port) {
        this.host = host;
        this.port = port;
    }

    @Override
    protected void waitUntilReady() {
        try {
            Unreliables.retryUntilTrue((int) startupTimeout.getSeconds(), TimeUnit.SECONDS,
                    () -> getRateLimiter().getWhenReady(this::ping));
        } catch (final org.rnorth.ducttape.TimeoutException e) {
            throw new ContainerLaunchException("Timed out waiting for PONG from " + host + ":" + port, e);
        }
    }

    private boolean ping() {
        try (Jedis jedis = new Jedis(host, port, 1000)) {
            return "PONG".equals(jedis.ping());
        } catch (final JedisException e) {
            return false;
        }
    }
}