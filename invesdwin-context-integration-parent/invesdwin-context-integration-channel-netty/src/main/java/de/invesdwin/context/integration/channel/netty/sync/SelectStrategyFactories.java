package de.invesdwin.context.integration.channel.netty.sync;

import javax.annotation.concurrent.Immutable;

import io.netty.channel.DefaultSelectStrategyFactory;
import io.netty.channel.SelectStrategy;
import io.netty.channel.SelectStrategyFactory;

@Immutable
public enum SelectStrategyFactories implements SelectStrategyFactory {
    DEFAULT {
        @Override
        public SelectStrategy newSelectStrategy() {
            return DefaultSelectStrategyFactory.INSTANCE.newSelectStrategy();
        }
    },
    BUSY_WAIT {
        @Override
        public SelectStrategy newSelectStrategy() {
            return (selectSupplier, hasTasks) -> SelectStrategy.BUSY_WAIT;
        }
    },
    SELECT {
        @Override
        public SelectStrategy newSelectStrategy() {
            return (selectSupplier, hasTasks) -> SelectStrategy.SELECT;
        }
    };

}
