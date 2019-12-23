package cn.yxffcode.yugo.obj.http;

import cn.yxffcode.yugo.obj.AbstractTableInsertExecutionLogic;
import cn.yxffcode.yugo.obj.ObjectTableModify;
import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.enumerable.EnumerableRelImplementor;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.MethodCallExpression;

import java.util.List;

/**
 * http接口调用的内部逻辑，{@link #getParameterIndex(ObjectTableModify, JavaTypeFactory)}方法用于创建SQL中的参数信息，
 * {@link #implement(ObjectTableModify, List, EnumerableRelImplementor, EnumerableRel.Prefer)}用于
 * 生成表示http的调用逻辑的{@link Expression}
 *
 * @see ObjectTableModify#implement(EnumerableRelImplementor, EnumerableRel.Prefer)
 * @author gaohang
 */
final class HttpTableInsertExecutionLogic extends AbstractTableInsertExecutionLogic {

  /**
   * @return {@link HttpTableInsertExecutionLogic}的单例对象
   * @see HttpTableInsertExecutionLogicHolder
   */
  static HttpTableInsertExecutionLogic getInstance() {
    return HttpTableInsertExecutionLogicHolder.INSTANCE;
  }

  @Override
  protected MethodCallExpression callInsert(final String schemaName, final String tableName) {
    return Expressions.call(
        HttpApiRuleMethods.HTTP_TABLE_INSERT.method,
        Expressions.constant(schemaName),
        Expressions.constant(tableName),
        DataContext.ROOT);
  }

  private static final class HttpTableInsertExecutionLogicHolder {
    private static final HttpTableInsertExecutionLogic INSTANCE =
        new HttpTableInsertExecutionLogic();
  }
}
