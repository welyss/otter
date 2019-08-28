/*
 * Copyright (C) 2010-2101 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.otter.node.etl.common.db.dialect;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.NestableRuntimeException;
import org.apache.ddlutils.model.Column;
import org.apache.ddlutils.model.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.support.lob.LobHandler;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.util.Assert;

import com.alibaba.fastsql.sql.ast.SQLDataType;
import com.alibaba.fastsql.sql.ast.SQLDataTypeImpl;
import com.alibaba.fastsql.sql.ast.SQLExpr;
import com.alibaba.fastsql.sql.ast.SQLStatement;
import com.alibaba.fastsql.sql.ast.expr.SQLCharExpr;
import com.alibaba.fastsql.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.fastsql.sql.ast.expr.SQLMethodInvokeExpr;
import com.alibaba.fastsql.sql.ast.expr.SQLNullExpr;
import com.alibaba.fastsql.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.fastsql.sql.ast.statement.SQLColumnConstraint;
import com.alibaba.fastsql.sql.ast.statement.SQLColumnDefinition;
import com.alibaba.fastsql.sql.ast.statement.SQLColumnPrimaryKey;
import com.alibaba.fastsql.sql.ast.statement.SQLColumnUniqueKey;
import com.alibaba.fastsql.sql.ast.statement.SQLCreateTableStatement;
import com.alibaba.fastsql.sql.ast.statement.SQLNotNullConstraint;
import com.alibaba.fastsql.sql.ast.statement.SQLNullConstraint;
import com.alibaba.fastsql.sql.ast.statement.SQLSelectOrderByItem;
import com.alibaba.fastsql.sql.ast.statement.SQLTableElement;
import com.alibaba.fastsql.sql.dialect.mysql.ast.MySqlPrimaryKey;
import com.alibaba.fastsql.sql.dialect.mysql.ast.MySqlUnique;
import com.alibaba.fastsql.sql.repository.Schema;
import com.alibaba.fastsql.sql.repository.SchemaObject;
import com.alibaba.fastsql.sql.repository.SchemaRepository;
import com.alibaba.fastsql.util.JdbcConstants;
import com.alibaba.otter.canal.parse.inbound.mysql.ddl.DruidDdlParser;
import com.alibaba.otter.node.etl.common.datasource.DataSourceService;
import com.alibaba.otter.shared.common.utils.meta.DdlUtils;
import com.alibaba.otter.shared.common.utils.meta.DdlUtilsFilter;
import com.google.common.base.Function;
import com.google.common.collect.OtterMigrateMap;

/**
 * @author jianghang 2011-10-27 下午01:50:19
 * @version 4.0.0
 */
public abstract class AbstractDbDialect implements DbDialect {

    protected static final Logger      logger = LoggerFactory.getLogger(AbstractDbDialect.class);
    protected int                      databaseMajorVersion;
    protected int                      databaseMinorVersion;
    protected String                   databaseName;
    protected DataSourceService        dataSourceService;
    protected SqlTemplate              sqlTemplate;
    protected JdbcTemplate             jdbcTemplate;
    protected TransactionTemplate      transactionTemplate;
    protected LobHandler               lobHandler;
    protected Map<List<String>, Table> tables;
    private SchemaRepository           repository = new SchemaRepository(JdbcConstants.MYSQL);

    public AbstractDbDialect(final JdbcTemplate jdbcTemplate, LobHandler lobHandler){
        this.jdbcTemplate = jdbcTemplate;
        this.lobHandler = lobHandler;
        // 初始化transction
        this.transactionTemplate = new TransactionTemplate();
        transactionTemplate.setTransactionManager(new DataSourceTransactionManager(jdbcTemplate.getDataSource()));
        transactionTemplate.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);

        // 初始化一些数据
        jdbcTemplate.execute(new ConnectionCallback() {

            public Object doInConnection(Connection c) throws SQLException, DataAccessException {
                DatabaseMetaData meta = c.getMetaData();
                databaseName = meta.getDatabaseProductName();
                databaseMajorVersion = meta.getDatabaseMajorVersion();
                databaseMinorVersion = meta.getDatabaseMinorVersion();

                return null;
            }
        });

        initTables(jdbcTemplate);
    }

    public AbstractDbDialect(JdbcTemplate jdbcTemplate, LobHandler lobHandler, String name, int majorVersion,
                             int minorVersion){
        this.jdbcTemplate = jdbcTemplate;
        this.lobHandler = lobHandler;
        // 初始化transction
        this.transactionTemplate = new TransactionTemplate();
        transactionTemplate.setTransactionManager(new DataSourceTransactionManager(jdbcTemplate.getDataSource()));
        transactionTemplate.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);

        this.databaseName = name;
        this.databaseMajorVersion = majorVersion;
        this.databaseMinorVersion = minorVersion;

        initTables(jdbcTemplate);
    }

    public Table findTable(String schema, String table, boolean useCache) {
        List<String> key = Arrays.asList(schema, table);
        if (useCache == false) {
            tables.remove(key);
        }

        return tables.get(key);
    }

    public Table findTable(String schema, String table) {
        return findTable(schema, table, true);
    }

    public void reloadTable(String schema, String table) {
        if (StringUtils.isNotEmpty(table)) {
            tables.remove(Arrays.asList(schema, table));
        } else {
            // 如果没有存在表名，则直接清空所有的table，重新加载
            tables.clear();
        }
    }

	public void cacheCreateDDL(String schema, String table, String ddl) {
		if (StringUtils.isNotEmpty(schema)) {
			repository.setDefaultSchema(schema);
			try {
				// druid暂时flush privileges语法解析有问题
				if (!StringUtils.startsWithIgnoreCase(StringUtils.trim(ddl), "flush")
						&& !StringUtils.startsWithIgnoreCase(StringUtils.trim(ddl), "grant")) {
					repository.console(ddl);
					Schema schemaRep = repository.findSchema(schema);
					if (schemaRep != null) {
						SchemaObject data = schemaRep.findTable(table);
						if (data != null) {
							SQLStatement statement = data.getStatement();
							if (statement != null) {
								if (statement instanceof SQLCreateTableStatement) {
									SQLCreateTableStatement statementCT = (SQLCreateTableStatement) statement;
									int size = statementCT.getTableElementList().size();
									if (size > 0) {
										Table tableMeta = null;
										if ((table != null) && (table.length() > 0)) {
											tableMeta = new Table();
											tableMeta.setSchema(schema);
											tableMeta.setName(table);
//							                tableMeta.setType((String) values.get("TABLE_TYPE"));
//							                tableMeta.setCatalog((String) values.get("TABLE_CAT"));
//							                tableMeta.setDescription((String) values.get("REMARKS"));
											for (int i = 0; i < size; ++i) {
												SQLTableElement element = statementCT.getTableElementList().get(i);
												// 
												Column col = new Column();
												if (element instanceof SQLColumnDefinition) {
													SQLColumnDefinition column = (SQLColumnDefinition) element;
													String name = getSqlName(column.getName());
													// String charset = getSqlName(column.getCharsetExpr());
													SQLDataType dataType = column.getDataType();
													String dataTypStr = dataType.getName();
													if (dataType.getArguments().size() > 0) {
														dataTypStr += "(";
														for (int j = 0; j < column.getDataType().getArguments().size(); j++) {
															if (j != 0) {
																dataTypStr += ",";
															}
															SQLExpr arg = column.getDataType().getArguments().get(j);
															dataTypStr += arg.toString();
														}
														dataTypStr += ")";
													}

													if (dataType instanceof SQLDataTypeImpl) {
														SQLDataTypeImpl dataTypeImpl = (SQLDataTypeImpl) dataType;
														if (dataTypeImpl.isUnsigned()) {
															dataTypStr += " unsigned";
														}

														if (dataTypeImpl.isZerofill()) {
															dataTypStr += " zerofill";
														}
														col.setTypeCode(dataTypeImpl.jdbcType());
														String typeName = dataType.getName();
												        if ((typeName != null) && StringUtils.containsIgnoreCase(typeName, "UNSIGNED")) {
												            // 濡傛灉涓簎nsigned锛屽線涓婅皟澶т竴涓噺绾э紝閬垮厤鏁版嵁婧㈠嚭
												            switch (col.getTypeCode()) {
												                case Types.TINYINT:
												                	col.setTypeCode(Types.SMALLINT);
												                    break;
												                case Types.SMALLINT:
												                	col.setTypeCode(Types.INTEGER);
												                    break;
												                case Types.INTEGER:
												                	col.setTypeCode(Types.BIGINT);
												                    break;
												                case Types.BIGINT:
												                	col.setTypeCode(Types.DECIMAL);
												                    break;
												                default:
												                    break;
												            }
												        }
													}
//														col.setType(dataType.getName());

													if (column.getDefaultExpr() == null || column.getDefaultExpr() instanceof SQLNullExpr) {
														col.setDefaultValue(null);
													} else {
														col.setDefaultValue(DruidDdlParser.unescapeQuotaName(getSqlName(column.getDefaultExpr())));
													}

													col.setName(name);
//														col.setTypeCode(dataType.getDbType().ordinal());
													col.setRequired(true);
													List<SQLColumnConstraint> constraints = column.getConstraints();
													for (SQLColumnConstraint constraint : constraints) {
														if (constraint instanceof SQLNotNullConstraint) {
															col.setRequired(false);
														} else if (constraint instanceof SQLNullConstraint) {
															col.setRequired(true);
														} else if (constraint instanceof SQLColumnPrimaryKey) {
															col.setPrimaryKey(true);
															col.setRequired(false);
														} else if (constraint instanceof SQLColumnUniqueKey) {
//							                                col.setPrimaryKey(true);
														}
													}
													tableMeta.addColumn(col);
												} else if (element instanceof MySqlPrimaryKey) {
													MySqlPrimaryKey column = (MySqlPrimaryKey) element;
													List<SQLSelectOrderByItem> pks = column.getColumns();
													for (SQLSelectOrderByItem pk : pks) {
														String name = getSqlName(pk.getExpr());
														for(Column colu : tableMeta.getColumns()) {
															if (colu.getName().equals(name)) {
																col.setPrimaryKey(true);
																col.setRequired(false);
															}
														}
													}
												} else if (element instanceof MySqlUnique) {
//							                        MySqlUnique column = (MySqlUnique) element;
//							                        List<SQLSelectOrderByItem> uks = column.getColumns();
//							                        for (SQLSelectOrderByItem uk : uks) {
//							                            String name = getSqlName(uk.getExpr());
//							                            FieldMeta field = tableMeta.getFieldMetaByName(name);
//							                            field.setUnique(true);
//							                        }
												}
											}
											if (tableMeta != null) {
												tables.put(Arrays.asList(schema, table), tableMeta);
											}
										}
									}
								}
							}
						}
					}
				}
			} catch (Throwable e) {
				logger.warn("parse faield : " + ddl, e);
			}
		}
	}

    private String getSqlName(SQLExpr sqlName) {
        if (sqlName == null) {
            return null;
        }

        if (sqlName instanceof SQLPropertyExpr) {
            SQLIdentifierExpr owner = (SQLIdentifierExpr) ((SQLPropertyExpr) sqlName).getOwner();
            return DruidDdlParser.unescapeName(owner.getName()) + "."
                   + DruidDdlParser.unescapeName(((SQLPropertyExpr) sqlName).getName());
        } else if (sqlName instanceof SQLIdentifierExpr) {
            return DruidDdlParser.unescapeName(((SQLIdentifierExpr) sqlName).getName());
        } else if (sqlName instanceof SQLCharExpr) {
            return ((SQLCharExpr) sqlName).getText();
        } else if (sqlName instanceof SQLMethodInvokeExpr) {
            return DruidDdlParser.unescapeName(((SQLMethodInvokeExpr) sqlName).getMethodName());
        } else {
            return sqlName.toString();
        }
    }

    public String getName() {
        return databaseName;
    }

    public int getMajorVersion() {
        return databaseMajorVersion;
    }

    @Override
    public int getMinorVersion() {
        return databaseMinorVersion;
    }

    public String getVersion() {
        return databaseMajorVersion + "." + databaseMinorVersion;
    }

    public LobHandler getLobHandler() {
        return lobHandler;
    }

    public JdbcTemplate getJdbcTemplate() {
        return jdbcTemplate;
    }

    public TransactionTemplate getTransactionTemplate() {
        return transactionTemplate;
    }

    public SqlTemplate getSqlTemplate() {
        return sqlTemplate;
    }

    public boolean isDRDS() {
        return false;
    }

    public String getShardColumns(String schema, String table) {
        return null;
    }

    public void destory() {
    }

    // ================================ helper method ==========================

    private void initTables(final JdbcTemplate jdbcTemplate) {
        this.tables = OtterMigrateMap.makeSoftValueComputingMap(new Function<List<String>, Table>() {

            public Table apply(List<String> names) {
                Assert.isTrue(names.size() == 2);
                try {
                    beforeFindTable(jdbcTemplate, names.get(0), names.get(0), names.get(1));
                    DdlUtilsFilter filter = getDdlUtilsFilter(jdbcTemplate, names.get(0), names.get(0), names.get(1));
                    Table table = DdlUtils.findTable(jdbcTemplate, names.get(0), names.get(0), names.get(1), filter);
                    afterFindTable(table, jdbcTemplate, names.get(0), names.get(0), names.get(1));
                    if (table == null) {
                        throw new NestableRuntimeException("no found table [" + names.get(0) + "." + names.get(1)
                                                           + "] , pls check");
                    } else {
                        return table;
                    }
                } catch (Exception e) {
                    throw new NestableRuntimeException("find table [" + names.get(0) + "." + names.get(1) + "] error",
                        e);
                }
            }
        });
    }

    protected DdlUtilsFilter getDdlUtilsFilter(JdbcTemplate jdbcTemplate, String catalogName, String schemaName,
                                               String tableName) {
        // we need to return null for backward compatibility
        return null;
    }

    protected void beforeFindTable(JdbcTemplate jdbcTemplate, String catalogName, String schemaName, String tableName) {
        // for subclass to extend
    }

    protected void afterFindTable(Table table, JdbcTemplate jdbcTemplate, String catalogName, String schemaName,
                                  String tableName) {
        // for subclass to extend
    }
}
