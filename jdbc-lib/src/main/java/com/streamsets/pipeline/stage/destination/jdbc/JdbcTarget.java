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
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.Target;
import com.streamsets.pipeline.api.base.BaseTarget;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.el.ELVars;
import com.streamsets.pipeline.lib.el.ELUtils;
import com.streamsets.pipeline.lib.jdbc.ChangeLogFormat;
import com.streamsets.pipeline.lib.jdbc.JdbcErrors;
import com.streamsets.pipeline.lib.jdbc.HikariPoolConfigBean;
import com.streamsets.pipeline.lib.jdbc.JdbcFieldColumnParamMapping;
import com.streamsets.pipeline.lib.jdbc.JdbcGenericRecordWriter;
import com.streamsets.pipeline.lib.jdbc.JdbcMultiRowRecordWriter;
import com.streamsets.pipeline.lib.jdbc.JdbcCustomQueryRecordWriter;
import com.streamsets.pipeline.lib.jdbc.JdbcRecordWriter;
import com.streamsets.pipeline.lib.jdbc.JdbcUtil;
import com.streamsets.pipeline.lib.jdbc.MicrosoftJdbcRecordWriter;
import com.streamsets.pipeline.stage.common.DefaultErrorRecordHandler;
import com.streamsets.pipeline.stage.common.ErrorRecordHandler;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
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
  private final String tableNameOrCustomQueryTemplate;
  private final List<JdbcFieldColumnParamMapping> customMappings;

  private final Properties driverProperties = new Properties();
  private final ChangeLogFormat changeLogFormat;
  private final HikariPoolConfigBean hikariConfigBean;

  private ErrorRecordHandler errorRecordHandler;
  private HikariDataSource dataSource = null;
  private ELEval evaluator;
  private ELVars variables;

  private Connection connection = null;

  class RecordWriterLoader extends CacheLoader<String, JdbcRecordWriter> {
    @Override
    public JdbcRecordWriter load(String key) throws Exception {
      return createRecordWriter(key);
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
      final String queryTamplate,
      final String tableNameTemplate,
      final List<JdbcFieldColumnParamMapping> customMappings,
      final boolean rollbackOnError,
      final boolean useMultiRowInsert,
      int maxPrepStmtParameters,
      final ChangeLogFormat changeLogFormat,
      final HikariPoolConfigBean hikariConfigBean
  ) {
      this.useCustomQuery = useCustomQuery;

      if (useCustomQuery) {
        this.tableNameOrCustomQueryTemplate = queryTamplate;
      } else {
        this.tableNameOrCustomQueryTemplate = tableNameTemplate;
      }

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

    variables = getContext().createELVars();

    if (useCustomQuery) {
      _initWithCustomQuery(context, issues);
    } else {
      _initWithTableName(context, issues);
    }

    return issues;
  }

  private void _initWithTableName(final Target.Context context, final List<ConfigIssue> issues) {
    evaluator = context.createELEval(JdbcUtil.TABLE_NAME);
    ELUtils.validateExpression(
            evaluator,
            variables,
            tableNameOrCustomQueryTemplate,
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
                tableNameOrCustomQueryTemplate,
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
    evaluator = getContext().createELEval(JdbcUtil.CUSTOM_QUERY);
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
        if (useCustomQuery) {
            recordWriter = new JdbcCustomQueryRecordWriter(
                    hikariConfigBean.connectionString,
                    dataSource,
                    rollbackOnError
            );
        } else {
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
    JdbcUtil.write(batch, evaluator, variables, tableNameOrCustomQueryTemplate, recordWriters, errorRecordHandler);
  }

}
