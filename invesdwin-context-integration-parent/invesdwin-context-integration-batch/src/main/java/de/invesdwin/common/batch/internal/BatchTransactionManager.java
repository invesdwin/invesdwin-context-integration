package de.invesdwin.common.batch.internal;

import javax.annotation.concurrent.ThreadSafe;

import org.springframework.transaction.PlatformTransactionManager;

import de.invesdwin.context.persistence.jpa.scanning.transaction.ADelegateTransactionManager;

@ThreadSafe
public class BatchTransactionManager extends ADelegateTransactionManager {

    @Override
    protected PlatformTransactionManager createDelegate() {
        return BatchDatabaseProperties.getPersistenceUnitContext().getTransactionManager();
    }
}
