package cn.yxffcode.yugo.obj.http;

import cn.yxffcode.yugo.obj.ParameterIndex;
import cn.yxffcode.yugo.obj.TableExecuteException;
import cn.yxffcode.yugo.obj.UnsupportedStatementException;
import com.google.common.collect.Lists;
import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.enumerable.EnumerableRelImplementor;
import org.apache.calcite.adapter.enumerable.JavaRowFormat;
import org.apache.calcite.adapter.enumerable.PhysType;
import org.apache.calcite.adapter.enumerable.PhysTypeImpl;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.interpreter.Bindables;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.ParameterExpression;
import org.apache.calcite.sql.SqlAbstractDateTimeLiteral;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.dialect.MysqlSqlDialect;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.util.Collections;
import java.util.List;

/**
 * http接口调用的内部逻辑，{@link #getParameterIndex(HttpTableModify, JavaTypeFactory)}方法用于创建SQL中的参数信息， {@link
 * #implement(HttpTableModify, List, EnumerableRelImplementor, EnumerableRel.Prefer)}用于
 * 生成表示http的调用逻辑的{@link Expression}
 *
 * @see HttpTableModify#implement(EnumerableRelImplementor, EnumerableRel.Prefer)
 * @author gaohang
 */
class HttpTableInsertExecutionLogic {

  private static void checkParamIndex(
      final int size, final int index, final SqlInsert insert, final SqlDialect dialect) {
    if (size <= index) {
      throw new IllegalStateException(
          "sql error, value count is great than column count, please check the sql: "
              + insert.toSqlString(dialect));
    }
  }

  /**
   * @return {@link HttpTableInsertExecutionLogic}的单例对象
   * @see HttpTableInsertExecutionLogicHolder
   */
  static HttpTableInsertExecutionLogic getInstance() {
    return HttpTableInsertExecutionLogicHolder.INSTANCE;
  }

  /**
   * 生成SQL中的参数信息
   *
   * @param tableModify 表示当前表的更新对象
   * @param typeFactory calcite的类型工厂
   * @return 当前更新语句中的参数信息
   */
  List<ParameterIndex> getParameterIndex(
      final HttpTableModify tableModify, final JavaTypeFactory typeFactory) {
    if (tableModify.getParameterIndexes() != null) {
      return tableModify.getParameterIndexes();
    }
    final SqlInsert insert = parseToSqlInsert(tableModify, typeFactory);
    final List<ParameterIndex> parameterIndices = doBuildParameterIndex(tableModify, insert);
    tableModify.setParameterIndexes(parameterIndices);
    return parameterIndices;
  }

  /** @see #getParameterIndex(HttpTableModify, JavaTypeFactory) */
  private SqlInsert parseToSqlInsert(
      final HttpTableModify tableModify, final JavaTypeFactory typeFactory) {
    final HttpTableInsertSqlConverter sqlConverter =
        new HttpTableInsertSqlConverter(MysqlSqlDialect.DEFAULT, typeFactory);
    final HttpTableInsertSqlConverter.Result result = sqlConverter.visitChild(0, tableModify);
    final SqlNode fullSqlStatement = result.asStatement();
    if (fullSqlStatement.getKind() != SqlKind.INSERT) {
      throw new UnsupportedStatementException("can only support INSERT dml operation");
    }
    return (SqlInsert) fullSqlStatement;
  }

  /** @see #getParameterIndex(HttpTableModify, JavaTypeFactory) */
  private List<ParameterIndex> doBuildParameterIndex(
      final HttpTableModify tableModify, final SqlInsert insert) {
    final List<SqlNode> columnList = insert.getTargetColumnList().getList();
    final SqlNode source = insert.getSource();

    return doBuildParameterIndex0(tableModify, insert, columnList, source);
  }

  private List<ParameterIndex> doBuildParameterIndex0(
      final HttpTableModify tableModify,
      final SqlInsert insert,
      final List<SqlNode> columnList,
      final SqlNode source) {
    if (source instanceof SqlSelect) {
      // prepared statement or insert select
      return processSubSelect(tableModify, insert, columnList, (SqlSelect) source);
    } else if (source instanceof SqlCall) {
      return processSqlCallTree(tableModify, (SqlCall) source, columnList, insert);
    }
    throw new UnsupportedStatementException("statement is not supported: " + insert.toString());
  }

  private List<ParameterIndex> processSqlCallTree(
      final HttpTableModify tableModify,
      final SqlCall sqlCall,
      final List<SqlNode> columnList,
      final SqlInsert insert) {
    final List<ParameterIndex> parameterIndexes = Lists.newArrayList();
    for (int i = 0; i < sqlCall.operandCount(); i++) {
      final SqlNode operand = sqlCall.operand(i);
      switch (operand.getKind()) {
        case SELECT:
          parameterIndexes.addAll(
              processSubSelect(tableModify, insert, columnList, (SqlSelect) operand));
          break;
        default:
          final ParameterIndex parameterIndex = new ParameterIndex();
          processSqlCallTree0(tableModify, operand, parameterIndex);
          parameterIndexes.add(parameterIndex);
      }
    }
    return parameterIndexes;
  }

  private void processSqlCallTree0(
      final HttpTableModify tableModify,
      final SqlNode sqlNode,
      final ParameterIndex parameterIndex) {

    if (sqlNode instanceof SqlCall) {
      final SqlCall sqlCall = (SqlCall) sqlNode;
      for (int i = 0; i < sqlCall.operandCount(); i++) {
        final SqlNode operand = sqlCall.operand(i);
        if (operand instanceof SqlCall) {
          processSqlCallTree0(tableModify, operand, parameterIndex);
        } else if (operand instanceof SqlLiteral) {
          final String columnName = tableModify.getTableDef().getColumnName(i);
          parameterIndex.addLast(columnName, getValue((SqlLiteral) operand), null);
        }
      }
    }
  }

  private List<ParameterIndex> processSubSelect(
      final HttpTableModify tableModify,
      final SqlInsert insert,
      final List<SqlNode> columnList,
      final SqlSelect select) {
    final ParameterIndex parameterIndex =
        new ParameterIndex(tableModify.getTableDef().getColumnCount());

    final SqlNode from = select.getFrom();
    if (from instanceof SqlScanNode) {
      final Bindables.BindableTableScan tableScan = ((SqlScanNode) from).getTableScan();
      parameterIndex.setSubSelect(tableScan);
    }

    final List<SqlNode> selectList = select.getSelectList().getList();
    for (int i = 0; i < selectList.size(); i++) {
      final SqlNode sqlNode = selectList.get(i);
      final Pair<Object, Integer> columnValue = getSqlNodeValue(insert, sqlNode);
      final String fieldName;
      if (columnList.isEmpty()) {
        // use tableDef
        fieldName = tableModify.getTableDef().getColumnName(i);
      } else {
        checkParamIndex(columnList.size(), i, insert, MysqlSqlDialect.DEFAULT);
        final SqlIdentifier column = (SqlIdentifier) columnList.get(i);
        final String name = column.names.get(0);
        final int idx = name.indexOf('$');
        if (idx >= 0) {
          final int columnIndex = Integer.parseInt(name.substring(idx + 1));
          fieldName = tableModify.getTableDef().getColumnName(columnIndex);
        } else {
          fieldName = name;
        }
      }

      parameterIndex.addLast(fieldName, columnValue);
    }
    return Collections.singletonList(parameterIndex);
  }

  /** @return value -> index */
  private Pair<Object, Integer> getSqlNodeValue(final SqlInsert insert, final SqlNode sqlNode) {
    switch (sqlNode.getKind()) {
      case DYNAMIC_PARAM:
        final SqlDynamicParam dynamicParam = (SqlDynamicParam) sqlNode;
        return ImmutablePair.of(ParameterIndex.dynamicValue(), dynamicParam.getIndex());
      case LITERAL:
        final SqlLiteral sqlLiteral = (SqlLiteral) sqlNode;
        return ImmutablePair.of(getValue(sqlLiteral), null);
      case AS:
        // 有嵌套
        final SqlCall sqlCall = (SqlCall) sqlNode;
        final SqlNode operand = sqlCall.operand(0);
        return getSqlNodeValue(insert, operand);
      default:
        throw new UnsupportedStatementException(
            "unsupported sql statement, only ? and value are supported, please check the sql: "
                + insert.toSqlString(MysqlSqlDialect.DEFAULT));
    }
  }

  private Object getValue(final SqlLiteral sqlLiteral) {
    final Object columnValue;
    if (sqlLiteral instanceof SqlCharStringLiteral) {
      columnValue = sqlLiteral.getStringValue();
    } else if (sqlLiteral instanceof SqlNumericLiteral) {
      columnValue = sqlLiteral.bigDecimalValue();
    } else if (sqlLiteral instanceof SqlAbstractDateTimeLiteral) {
      columnValue = ((SqlAbstractDateTimeLiteral) sqlLiteral).toFormattedString();
    } else {
      columnValue = sqlLiteral.getValue().toString();
    }
    return columnValue;
  }

  /**
   * 将表更新转化成代码逻辑，返回的{@link org.apache.calcite.adapter.enumerable.EnumerableRel.Result}
   * 会被calcite用于生成Java对象
   *
   * @param tableModify 表示当前的更新语句
   * @param parameterIndex 参数信息，见{@link #getParameterIndex(HttpTableModify, JavaTypeFactory)}
   * @param implementor 执行计划的实现器
   * @param pref Preferred physical type
   * @return 表示执行计划的代码
   */
  EnumerableRel.Result implement(
      final HttpTableModify tableModify,
      final List<ParameterIndex> parameterIndex,
      final EnumerableRelImplementor implementor,
      final EnumerableRel.Prefer pref) {

    final BlockBuilder builder = new BlockBuilder();
    final PhysType physType =
        PhysTypeImpl.of(
            implementor.getTypeFactory(),
            tableModify.getRowType(),
            pref.prefer(JavaRowFormat.CUSTOM));

    // Enumerable enumerable = HttpReqTableInsertInvoker.execute(schemaName, tableName, datacontext,
    // insertParameterIndex);
    final List<String> qualifiedName = tableModify.getTable().getQualifiedName();
    Expression enumerable =
        builder.append(
            "enumerable",
            Expressions.call(
                HttpApiRuleMethods.HTTP_TABLE_INSERT.method,
                Expressions.constant(qualifiedName.get(0)),
                Expressions.constant(qualifiedName.get(1)),
                DataContext.ROOT));
    // return enumerable;
    builder.add(Expressions.return_(null, enumerable));

    final BlockBuilder builder0 = new BlockBuilder(false);

    final ParameterExpression e_ = Expressions.parameter(Exception.class, builder.newName("e"));
    builder0.add(
        Expressions.tryCatch(
            builder.toBlock(),
            Expressions.catch_(
                e_, Expressions.throw_(Expressions.new_(TableExecuteException.class, e_)))));
    implementor.map.put(ParameterIndex.CONTEXT_PARAMETER_KEY, parameterIndex);
    return implementor.result(physType, builder0.toBlock());
  }

  private static final class HttpTableInsertExecutionLogicHolder {
    private static final HttpTableInsertExecutionLogic INSTANCE =
        new HttpTableInsertExecutionLogic();
  }
}
