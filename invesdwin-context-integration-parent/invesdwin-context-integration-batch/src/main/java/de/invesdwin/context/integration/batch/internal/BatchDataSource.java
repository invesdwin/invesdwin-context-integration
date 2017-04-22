package de.invesdwin.context.integration.batch.internal;

import javax.annotation.concurrent.ThreadSafe;
import javax.sql.DataSource;

import de.invesdwin.context.persistence.jpa.scanning.datasource.ADelegateDataSource;

@ThreadSafe
public class BatchDataSource extends ADelegateDataSource {

    @Override
    protected DataSource createDelegate() {
        final DataSource dataSource = BatchDatabaseProperties.getPersistenceUnitContext().getDataSource();
        return dataSource;
    }

}
