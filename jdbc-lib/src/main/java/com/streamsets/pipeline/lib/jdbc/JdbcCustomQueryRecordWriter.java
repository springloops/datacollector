package com.streamsets.pipeline.lib.jdbc;

import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by min on 2016. 9. 23..
 */
public class JdbcCustomQueryRecordWriter implements JdbcRecordWriter {
    private static final Logger LOG = LoggerFactory.getLogger(JdbcCustomQueryRecordWriter.class);

    private final String connectionString;
    private final HikariDataSource dataSource;
    private final boolean rollbackOnError;

    public JdbcCustomQueryRecordWriter(
            String connectionString,
            HikariDataSource dataSource,
            boolean rollbackOnError) {
        this.connectionString = connectionString;
        this.dataSource = dataSource;
        this.rollbackOnError = rollbackOnError;
    }

    @Override
    public List<OnRecordErrorException> writeBatch(Collection<Record> batch) throws StageException {
        List<OnRecordErrorException> errorRecords = new LinkedList<>();
        try (Connection connection = dataSource.getConnection()) {
            writeRecordByQuery(connection, batch);
        } catch (OnRecordErrorException error) {
            errorRecords.add(error);
        } catch (SQLException e) {
            String formattedError = JdbcUtil.formatSqlException(e);
            LOG.error(formattedError, e);
            LOG.error("Query failed at: {}", System.currentTimeMillis());
        }

        return errorRecords;
    }

    private void writeRecordByQuery(Connection connection, Collection<Record> batch) throws StageException {

        int size = batch.size();
        Iterator<Record> iterator = batch.iterator();
        Record record = null;
        String query = null;

        try (Statement stmt = connection.createStatement()) {
            while (iterator.hasNext()) {
                record = iterator.next();
                query = record.get(JdbcUtil.CUSTOM_QUERY_FIELD_PATH).getValueAsString();
                LOG.debug("Query String in Record : {}", query);
                stmt.addBatch(query);
            }

            int[] result = stmt.executeBatch();

            if (size == result.length) {
                // todo commit
            } else {
                LOG.error(JdbcErrors.JDBC_14.getMessage(), query);
                throw new OnRecordErrorException(record, JdbcErrors.JDBC_14, query);
            }

        } catch (SQLException e) {
            // Exception executing query
            LOG.error(JdbcErrors.JDBC_02.getMessage(), query, e);
            throw new OnRecordErrorException(record, JdbcErrors.JDBC_02, query, e.getMessage());
        }

    }
}
