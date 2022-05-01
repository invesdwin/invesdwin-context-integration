package de.invesdwin.context.integration.batch.internal;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.log.error.Err;
import de.invesdwin.context.persistence.jpa.ConnectionAutoSchema;
import de.invesdwin.context.persistence.jpa.ConnectionDialect;
import de.invesdwin.context.persistence.jpa.PersistenceProperties;
import de.invesdwin.context.persistence.jpa.PersistenceUnitContext;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.error.UnknownArgumentException;

@Immutable
public final class BatchDatabaseProperties {

    private BatchDatabaseProperties() {
    }

    public static PersistenceUnitContext getPersistenceUnitContext() {
        return PersistenceProperties.getPersistenceUnitContext(PersistenceProperties.DEFAULT_PERSISTENCE_UNIT_NAME);
    }

    public static boolean isInitDatabaseEnabled() {
        final ConnectionAutoSchema connectionAutoSchema = getPersistenceUnitContext().getConnectionAutoSchema();
        switch (connectionAutoSchema) {
        case CREATE:
        case CREATE_DROP:
        case UPDATE:
            return !isDatabaseAlreadyInitialized();
        case VALIDATE:
            return false;
        default:
            throw UnknownArgumentException.newInstance(ConnectionAutoSchema.class, connectionAutoSchema);
        }
    }

    private static boolean isDatabaseAlreadyInitialized() {
        final BatchDataSource dataSource = new BatchDataSource();
        try {
            try (Connection connection = dataSource.getConnection()) {
                final DatabaseMetaData metadata = connection.getMetaData();
                try (ResultSet resultSet = metadata.getTables(null, null, "BATCH_JOB_EXECUTION", null)) {
                    final boolean tableExists = resultSet.next();
                    return tableExists;
                }
            }
        } catch (final SQLException e) {
            throw Err.process(e);
        }
    }

    public static boolean isDestroyDatabaseEnabled() {
        final ConnectionAutoSchema connectionAutoSchema = getPersistenceUnitContext().getConnectionAutoSchema();
        switch (connectionAutoSchema) {
        case CREATE_DROP:
            return true;
        case CREATE:
        case UPDATE:
        case VALIDATE:
            return false;
        default:
            throw UnknownArgumentException.newInstance(ConnectionAutoSchema.class, connectionAutoSchema);
        }
    }

    public static String getDatabaseTypeForSchemaScript() {
        final ConnectionDialect connectionDialect = getPersistenceUnitContext().getConnectionDialect();
        Assertions.assertThat(connectionDialect.isRdbms())
                .as("Only RDBMS %ss are supported by spring-batch. %s", ConnectionDialect.class.getSimpleName(),
                        connectionDialect)
                .isTrue();
        switch (connectionDialect) {
        case H2:
            return "h2";
        case HSQLDB:
            return "hsqldb";
        case MYSQL:
            return "mysql";
        case POSTGRESQL:
            return "postgresql";
        case ORACLE:
            return "oracle10g";
        case MSSQLSERVER:
            return "sqlserver";
        case DERBY:
            return "derby";
        case SYBASE:
            return "sybase";
        default:
            throw UnknownArgumentException.newInstance(ConnectionDialect.class, connectionDialect);
        }
    }

}
