/**
 * Copyright 2015 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.stage.destination.jdbc;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.Target;
import com.streamsets.pipeline.api.base.BaseTarget;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.el.ELEvalException;
import com.streamsets.pipeline.api.el.ELVars;
import com.streamsets.pipeline.lib.el.ELUtils;
import com.streamsets.pipeline.lib.el.RecordEL;
import com.streamsets.pipeline.lib.jdbc.*;
import com.streamsets.pipeline.stage.common.DefaultErrorRecordHandler;
import com.streamsets.pipeline.stage.common.ErrorRecordHandler;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * JDBC Destination for StreamSets Data Collector
 */
public class JdbcTarget extends BaseTarget {
  private static final Logger LOG = LoggerFactory.getLogger(JdbcTarget.class);

  private static final String HIKARI_CONFIG_PREFIX = "hikariConfigBean.";
  private static final String CONNECTION_STRING = HIKARI_CONFIG_PREFIX + "connectionString";

  private final boolean rollbackOnError;
  private final boolean useMultiRowInsert;
  private final int maxPrepStmtParameters;

  private final boolean useCustomQuery;
  private final String query;
  private ELEval queryEval;

  private final String tableNameTemplate;
  private final List<JdbcFieldColumnParamMapping> customMappings;

  private final Properties driverProperties = new Properties();
  private final ChangeLogFormat changeLogFormat;
  private final HikariPoolConfigBean hikariConfigBean;

  private ErrorRecordHandler errorRecordHandler;
  private HikariDataSource dataSource = null;
  private ELEval tableNameEval = null;
  private ELVars tableNameVars = null;

  private Connection connection = null;

  class RecordWriterLoader extends CacheLoader<String, JdbcRecordWriter> {
    @Override
    public JdbcRecordWriter load(String tableName) throws Exception {
      return createRecordWriter(tableName);
    }
  }

  private final LoadingCache<String, JdbcRecordWriter> recordWriters = CacheBuilder.newBuilder()
      .maximumSize(500)
      .expireAfterAccess(1, TimeUnit.HOURS)
      .build(new RecordWriterLoader());

  public JdbcTarget(
      final String tableNameTemplate,
      final List<JdbcFieldColumnParamMapping> customMappings,
      final boolean rollbackOnError,
      final boolean useMultiRowInsert,
      int maxPrepStmtParameters,
      final ChangeLogFormat changeLogFormat,
      final HikariPoolConfigBean hikariConfigBean
  ) {
    this(
        false,
        null,
        tableNameTemplate,
        customMappings,
        rollbackOnError,
        useMultiRowInsert,
        maxPrepStmtParameters,
        changeLogFormat,
        hikariConfigBean
    );
  }

  public JdbcTarget(
          final boolean useCustomQuery,
          final String query,
          final String tableNameTemplate,
          final List<JdbcFieldColumnParamMapping> customMappings,
          final boolean rollbackOnError,
          final boolean useMultiRowInsert,
          int maxPrepStmtParameters,
          final ChangeLogFormat changeLogFormat,
          final HikariPoolConfigBean hikariConfigBean
  ) {
    this.useCustomQuery = useCustomQuery;
    this.query = query;
    this.tableNameTemplate = tableNameTemplate;
    this.customMappings = customMappings;
    this.rollbackOnError = rollbackOnError;
    this.useMultiRowInsert = useMultiRowInsert;
    this.maxPrepStmtParameters = maxPrepStmtParameters;
    this.driverProperties.putAll(hikariConfigBean.driverProperties);
    this.changeLogFormat = changeLogFormat;
    this.hikariConfigBean = hikariConfigBean;
  }

  @Override
  protected List<ConfigIssue> init() {
    List<ConfigIssue> issues = super.init();
    errorRecordHandler = new DefaultErrorRecordHandler(getContext());

    Target.Context context = getContext();

    issues = hikariConfigBean.validateConfigs(context, issues);

    if (useCustomQuery) {
      _initWithCustomQuery(context, issues);
    } else {
      _initWithTableName(context, issues);
    }

    return issues;
  }

  private void _initWithTableName(final Target.Context context, final List<ConfigIssue> issues) {
    tableNameVars = getContext().createELVars();
    tableNameEval = context.createELEval(JdbcUtil.TABLE_NAME);
    ELUtils.validateExpression(
            tableNameEval,
            tableNameVars,
            tableNameTemplate,
            getContext(),
            Groups.JDBC.getLabel(),
            JdbcUtil.TABLE_NAME,
            JdbcErrors.JDBC_26,
            String.class,
            issues
    );

    if (issues.isEmpty() && null == dataSource) {
      try {
        dataSource = JdbcUtil.createDataSourceForWrite(
                hikariConfigBean,
                driverProperties,
                tableNameTemplate,
                issues,
                customMappings,
                getContext()
        );
      } catch (RuntimeException | SQLException e) {
        LOG.debug("Could not connect to data source", e);
        issues.add(getContext().createConfigIssue(Groups.JDBC.name(), CONNECTION_STRING, JdbcErrors.JDBC_00, e.toString()));
      }
    }
  }

  private void _initWithCustomQuery(final Target.Context context, final List<ConfigIssue> issues) {
    queryEval = getContext().createELEval("query");
    if (issues.isEmpty() && null == dataSource) {
      try {
        dataSource = JdbcUtil.createDataSourceForRead(hikariConfigBean, driverProperties);
      } catch (StageException e) {
        issues.add(context.createConfigIssue(Groups.JDBC.name(), CONNECTION_STRING, JdbcErrors.JDBC_00, e.toString()));
      }
    }
  }

  @Override
  public void destroy() {
    JdbcUtil.closeQuietly(connection);

    if (null != dataSource) {
      dataSource.close();
    }
    super.destroy();
  }

  private JdbcRecordWriter createRecordWriter(String tableName) throws StageException {
    JdbcRecordWriter recordWriter;

    switch (changeLogFormat) {
      case NONE:
        if (!useMultiRowInsert) {
          recordWriter = new JdbcGenericRecordWriter(
              hikariConfigBean.connectionString,
              dataSource,
              tableName,
              rollbackOnError,
              customMappings
          );
        } else {
          recordWriter = new JdbcMultiRowRecordWriter(
              hikariConfigBean.connectionString,
              dataSource,
              tableName,
              rollbackOnError,
              customMappings,
              maxPrepStmtParameters
          );
        }
        break;
      case MSSQL:
        recordWriter = new MicrosoftJdbcRecordWriter(dataSource, tableName);
        break;
      default:
        throw new IllegalStateException("Unrecognized format specified: " + changeLogFormat);
    }
    return recordWriter;
  }

  @Override
  @SuppressWarnings("unchecked")
  public void write(Batch batch) throws StageException {
    if (useCustomQuery) {
      try {
        connection = dataSource.getConnection();
        Iterator<Record> batchIterator = batch.getRecords();

        while (batchIterator.hasNext()) {
          Record record = batchIterator.next();
          writeUseCustomQuery(record);
        }
      } catch (OnRecordErrorException error) {
        errorRecordHandler.onError(error);
      } catch (SQLException e) {
        String formattedError = JdbcUtil.formatSqlException(e);
        LOG.error(formattedError, e);
        LOG.error("Query failed at: {}", System.currentTimeMillis());
      } finally {
        JdbcUtil.closeQuietly(connection);
      }

    } else {
      JdbcUtil.write(batch, tableNameEval, tableNameVars, tableNameTemplate, recordWriters, errorRecordHandler);
    }

  }

  private void writeUseCustomQuery(Record record) throws StageException {
    ELVars elVars = getContext().createELVars();
    RecordEL.setRecordInContext(elVars, record);

    String preparedQuery;

    try {
      preparedQuery = queryEval.eval(elVars, query, String.class);
    } catch (ELEvalException e) {
      LOG.error(JdbcErrors.JDBC_01.getMessage(), query, e);
      throw new OnRecordErrorException(record, JdbcErrors.JDBC_01, query);
    }
  }

}
