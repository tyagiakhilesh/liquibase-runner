package com.mf.liquibase;

import liquibase.Contexts;
import liquibase.LabelExpression;
import liquibase.Liquibase;
import liquibase.database.Database;
import liquibase.database.DatabaseFactory;
import liquibase.database.jvm.JdbcConnection;
import liquibase.exception.DatabaseException;
import liquibase.exception.LiquibaseException;
import liquibase.resource.ClassLoaderResourceAccessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;
import java.util.Optional;

public class LiquibaseRunner {
    private static Logger logger = LoggerFactory.getLogger(LiquibaseRunner.class);

    private DataSource dataSource;

    public LiquibaseRunner(final DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public void runChangeLog(final String changelog, final Optional<Map<String, Object>> properties)
            throws SQLException, LiquibaseException {
        try (final Connection connection = dataSource.getConnection()) {
            final DatabaseFactory databaseFactory = DatabaseFactory.getInstance();
            final Database database = databaseFactory.findCorrectDatabaseImplementation(new JdbcConnection(connection));
            final Liquibase liquibase = new Liquibase(changelog, new ClassLoaderResourceAccessor(), database);
            properties.ifPresent(stringObjectMap -> stringObjectMap.entrySet().stream()
                    .forEach(entry -> liquibase.setChangeLogParameter(entry.getKey(), entry.getValue())));
            liquibase.update(new Contexts(""), new LabelExpression(""));
        } catch (SQLException | DatabaseException throwables) {
            logger.error("Error running changelog.", throwables);
            throw throwables;
        } catch (LiquibaseException e) {
            logger.error("Error running changelog.", e);
            throw e;
        }
    }
}
