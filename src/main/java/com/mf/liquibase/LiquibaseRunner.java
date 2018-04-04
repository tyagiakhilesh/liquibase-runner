package com.mf.liquibase;

import liquibase.Contexts;
import liquibase.LabelExpression;
import liquibase.Liquibase;
import liquibase.database.Database;
import liquibase.database.DatabaseFactory;
import liquibase.database.jvm.JdbcConnection;
import liquibase.resource.ClassLoaderResourceAccessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.util.Map;
import java.util.Optional;

public class LiquibaseRunner {
    private static Logger LOGGER = LoggerFactory.getLogger(LiquibaseRunner.class);

    private DataSource dataSource;

    public LiquibaseRunner(final DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public void runChangeLog(final String changelog, final Optional<Map<String, Object>> changeLogProperties) {
        try(Connection connection = dataSource.getConnection()) {
            final DatabaseFactory databaseFactory = DatabaseFactory.getInstance();
            final Database database = databaseFactory.findCorrectDatabaseImplementation(new JdbcConnection(connection));
            final Liquibase liquibase = new Liquibase(changelog, new ClassLoaderResourceAccessor(), database);
            if (changeLogProperties.isPresent()) {
                changeLogProperties.get().entrySet().stream().forEach(entry -> {
                    liquibase.setChangeLogParameter(entry.getKey(), entry.getValue());
                });
            }
            liquibase.update(new Contexts(""), new LabelExpression(""));
        } catch (final Exception ex) {
            LOGGER.error("Running changelogs.", ex);
        }
    }
}
