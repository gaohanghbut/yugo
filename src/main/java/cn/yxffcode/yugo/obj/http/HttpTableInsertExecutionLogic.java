package cn.yxffcode.yugo.obj.http;

import cn.yxffcode.yugo.obj.ParameterIndex;
import cn.yxffcode.yugo.obj.TableExecuteException;
import cn.yxffcode.yugo.obj.UnsupportedStatementException;
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
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.dialect.MysqlSqlDialect;

import java.util.List;

/**
 * http接口调用的内部逻辑，{@link #getParameterIndex(HttpTableModify, JavaTypeFactory)}方法用于创建SQL中的参数信息， {@link
 * #implement(HttpTableModify, ParameterIndex, EnumerableRelImplementor, EnumerableRel.Prefer)}用于
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
  ParameterIndex getParameterIndex(
      final HttpTableModify tableModify, final JavaTypeFactory typeFactory) {
    if (tableModify.getParameterIndex() != null) {
      return tableModify.getParameterIndex();
    }
    final SqlInsert insert = parseToSqlInsert(tableModify, typeFactory);
    final ParameterIndex parameterIndex = doBuildParameterIndex(tableModify, insert);
    tableModify.setParameterIndex(parameterIndex);
    return parameterIndex;
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
  private ParameterIndex doBuildParameterIndex(
      final HttpTableModify tableModify, final SqlInsert insert) {
    final ParameterIndex parameterIndex =
        new ParameterIndex(tableModify.getTableDef().getColumnCount());
    final List<SqlNode> columnList = insert.getTargetColumnList().getList();
    final SqlSelect select = (SqlSelect) insert.getSource();

    final SqlNode from = select.getFrom();
    if (from instanceof SqlScanNode) {
      final Bindables.BindableTableScan tableScan = ((SqlScanNode) from).getTableScan();
      parameterIndex.setSubSelect(tableScan);
    }

    final List<SqlNode> selectList = select.getSelectList().getList();
    for (int i = 0; i < selectList.size(); i++) {
      final SqlNode sqlNode = selectList.get(i);
      if (!(sqlNode instanceof SqlCall)) {
        continue;
      }
      final SqlCall sqlCall = (SqlCall) sqlNode;
      final SqlNode operand = sqlCall.operand(0);
      final Object columnValue;
      switch (operand.getKind()) {
        case DYNAMIC_PARAM:
          columnValue = ParameterIndex.dynamicValue();
          break;
        case LITERAL:
          final SqlLiteral sqlLiteral = (SqlLiteral) operand;
          if (sqlLiteral instanceof SqlCharStringLiteral) {
            columnValue = sqlLiteral.getStringValue();
          } else if (sqlLiteral instanceof SqlNumericLiteral) {
            columnValue = sqlLiteral.bigDecimalValue();
          } else if (sqlLiteral instanceof SqlAbstractDateTimeLiteral) {
            columnValue = ((SqlAbstractDateTimeLiteral) sqlLiteral).toFormattedString();
          } else {
            columnValue = sqlLiteral.getValue().toString();
          }
          break;
        default:
          throw new UnsupportedStatementException(
              "unsupported sql statement, only ? and value are supported, please check the sql: "
                  + insert.toSqlString(MysqlSqlDialect.DEFAULT));
      }
      final String fieldName;
      if (columnList.isEmpty()) {
        // use tableDef
        fieldName = tableModify.getTableDef().getColumnName(i);
      } else {
        checkParamIndex(columnList.size(), i, insert, MysqlSqlDialect.DEFAULT);
        final SqlIdentifier column = (SqlIdentifier) columnList.get(i);
        fieldName = column.names.get(0);
      }

      parameterIndex.addLast(fieldName, columnValue);
    }
    return parameterIndex;
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
      final ParameterIndex parameterIndex,
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
