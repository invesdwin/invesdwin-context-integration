package de.invesdwin.context.integration.channel.redis;

import javax.annotation.concurrent.Immutable;

import org.junit.platform.suite.api.SelectClasses;
import org.junit.platform.suite.api.Suite;

import de.invesdwin.context.integration.channel.redis.jedis.JedisChannelTest;

@Suite
@SelectClasses({ JedisChannelTest.class })
@Immutable
public class RedisChannelTestSuite {

}
