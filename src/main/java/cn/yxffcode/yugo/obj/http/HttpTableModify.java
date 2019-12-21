package cn.yxffcode.yugo.obj.http;

import cn.yxffcode.yugo.obj.ParameterIndex;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.enumerable.EnumerableRelImplementor;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;

import java.util.List;

/**
 * 处理dml语句
 *
 * @see HttpTableInsertExecutionLogic
 * @author gaohang
 */
class HttpTableModify extends TableModify implements EnumerableRel {

  /** 表定义 */
  private final HttpTableDef tableDef;

  /**
   * 参数信息
   *
   * @see HttpTableInsertExecutionLogic#getParameterIndex(HttpTableModify, JavaTypeFactory)
   */
  private ParameterIndex parameterIndex;

  /**
   * Creates a {@code TableModify}.
   *
   * <p>The UPDATE operation has format like this:
   *
   * <blockquote>
   *
   * <pre>UPDATE table SET iden1 = exp1, ident2 = exp2  WHERE condition</pre>
   *
   * </blockquote>
   *
   * @param cluster Cluster this relational expression belongs to
   * @param traitSet Traits of this relational expression
   * @param table Target table to modify
   * @param catalogReader accessor to the table metadata.
   * @param input Sub-query or filter condition
   * @param operation Modify operation (INSERT, UPDATE, DELETE)
   * @param updateColumnList List of column identifiers to be updated (e.g. ident1, ident2); null if
   *     not UPDATE
   * @param sourceExpressionList List of value expressions to be set (e.g. exp1, exp2); null if not
   *     UPDATE
   * @param flattened Whether set flattens the input row type
   */
  protected HttpTableModify(
      final HttpTableDef tableDef,
      final RelOptCluster cluster,
      final RelTraitSet traitSet,
      final RelOptTable table,
      final Prepare.CatalogReader catalogReader,
      final RelNode input,
      final Operation operation,
      final List<String> updateColumnList,
      final List<RexNode> sourceExpressionList,
      final boolean flattened) {
    super(
        cluster,
        traitSet,
        table,
        catalogReader,
        input,
        operation,
        updateColumnList,
        sourceExpressionList,
        flattened);
    this.tableDef = tableDef;
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
    return super.computeSelfCost(planner, mq).multiplyBy(.1);
  }

  public HttpTableDef getTableDef() {
    return tableDef;
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new HttpTableModify(
        tableDef,
        getCluster(),
        traitSet,
        getTable(),
        getCatalogReader(),
        sole(inputs),
        getOperation(),
        getUpdateColumnList(),
        getSourceExpressionList(),
        isFlattened());
  }

  @Override
  public Result implement(final EnumerableRelImplementor implementor, final Prefer pref) {
    final ParameterIndex parameterIndex =
        HttpTableInsertExecutionLogic.getInstance()
            .getParameterIndex(this, implementor.getTypeFactory());
    return HttpTableInsertExecutionLogic.getInstance()
        .implement(this, parameterIndex, implementor, pref);
  }

  ParameterIndex getParameterIndex() {
    return parameterIndex;
  }

  void setParameterIndex(final ParameterIndex parameterIndex) {
    this.parameterIndex = parameterIndex;
  }
}
