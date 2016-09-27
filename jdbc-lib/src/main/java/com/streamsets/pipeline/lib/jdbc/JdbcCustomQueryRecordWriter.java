package com.streamsets.pipeline.lib.jdbc;

import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
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
    private final boolean useMultiRowInsert;

    public JdbcCustomQueryRecordWriter(
            String connectionString,
            HikariDataSource dataSource,
            boolean rollbackOnError,
            boolean useMultiRowInsert) {
        this.connectionString = connectionString;
        this.dataSource = dataSource;
        this.rollbackOnError = rollbackOnError;
        this.useMultiRowInsert = useMultiRowInsert;
    }

    @Override
    public List<OnRecordErrorException> writeBatch(Collection<Record> batch) throws StageException {
        List<OnRecordErrorException> errorRecords = new LinkedList<>();
        try (Connection connection = dataSource.getConnection()) {
            writeRecordByQuery(connection, errorRecords, batch);
        } catch (SQLException e) {
            handleSqlException(e);
        }
        return errorRecords;
    }

    private void writeRecordByQuery(
            Connection connection,
            List<OnRecordErrorException> errorRecords,
            Collection<Record> batch) throws StageException {

        Iterator<Record> iterator = batch.iterator();

        try (Statement stmt = connection.createStatement()) {
            while (iterator.hasNext()) {
                Record record = iterator.next();
                String query = record.get(JdbcUtil.CUSTOM_QUERY_FIELD_PATH).getValueAsString();
                stmt.addBatch(query);
            }

            try {
                stmt.executeBatch();
            } catch (SQLException e) {
                if (rollbackOnError) {
                    connection.rollback();
                }
                handleBatchUpdateException(batch, e, errorRecords);
            }
            connection.commit();
        } catch (SQLException e) {
            handleSqlException(e);
        }
    }

    /**
     * This is an error that is not due to bad input record and should throw a StageException
     * once we format the error.
     * @param e SQLException
     * @throws StageException
     */
    private void handleSqlException(SQLException e) throws StageException {
        String formattedError = JdbcUtil.formatSqlException(e);
        LOG.error(formattedError);
        LOG.debug(formattedError, e);
        throw new StageException(JdbcErrors.JDBC_14, formattedError);
    }

    /**
     * <p>
     *   Some databases drivers allow us to figure out which record in a particular batch failed.
     * </p>
     * <p>
     *   In the case that we have a list of update counts, we can mark just the record as erroneous.
     *   Otherwise we must send the entire batch to error.
     * </p>
     * @param failedRecords List of Failed Records
     * @param e BatchUpdateException
     * @param errorRecords List of error records for this batch
     */
    private void handleBatchUpdateException(
            Collection<Record> failedRecords, SQLException e, List<OnRecordErrorException> errorRecords
    ) throws StageException {
        if (JdbcUtil.isDataError(connectionString, e)) {
            String formattedError = JdbcUtil.formatSqlException(e);
            LOG.error(formattedError);
            LOG.debug(formattedError, e);

            if (!rollbackOnError && e instanceof BatchUpdateException &&
                    ((BatchUpdateException) e).getUpdateCounts().length > 0) {
                BatchUpdateException bue = (BatchUpdateException) e;

                int i = 0;
                for (Record record : failedRecords) {
                    if (i >= bue.getUpdateCounts().length || bue.getUpdateCounts()[i] == PreparedStatement.EXECUTE_FAILED) {
                        errorRecords.add(new OnRecordErrorException(record, JdbcErrors.JDBC_14, formattedError));
                    }
                    i++;
                }
            } else {
                for (Record record : failedRecords) {
                    errorRecords.add(new OnRecordErrorException(record, JdbcErrors.JDBC_14, formattedError));
                }
            }
        } else {
            handleSqlException(e);
        }
    }
}
