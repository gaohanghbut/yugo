package cn.yxffcode.yugo.obj;

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.schema.ModifiableTable;
import org.apache.calcite.tools.RelBuilderFactory;

import java.util.function.Predicate;

/**
 * 将{@link TableModify}转换成{@link ObjectTableModify}
 *
 * @author gaohang
 */
public class ObjectTableModificationRule extends ConverterRule {
  private final TableDef tableRef;
  private final TableInsertExecutionLogic tableInsertExecutionLogic;

  public ObjectTableModificationRule(
      final TableDef tableRef,
      final RelBuilderFactory relBuilderFactory,
      final TableInsertExecutionLogic tableInsertExecutionLogic) {
    super(
        TableModify.class,
        (Predicate<RelNode>) r -> true,
        EnumerableConvention.INSTANCE,
        EnumerableConvention.INSTANCE,
        relBuilderFactory,
        "SqlToHttpPostConverterRule");
    this.tableRef = tableRef;
    this.tableInsertExecutionLogic = tableInsertExecutionLogic;
  }

  @Override
  public RelNode convert(final RelNode rel) {
    final TableModify modify = (TableModify) rel;
    if (modify instanceof ObjectTableModify) {
      return modify;
    }
    final ModifiableTable modifiableTable = modify.getTable().unwrap(ModifiableTable.class);
    if (modifiableTable == null) {
      return null;
    }
    final RelTraitSet traitSet = modify.getTraitSet().replace(EnumerableConvention.INSTANCE);
    return new ObjectTableModify(
        tableRef,
        modify.getCluster(),
        traitSet,
        modify.getTable(),
        modify.getCatalogReader(),
        convert(modify.getInput(), traitSet),
        modify.getOperation(),
        modify.getUpdateColumnList(),
        modify.getSourceExpressionList(),
        modify.isFlattened(),
        tableInsertExecutionLogic);
  }
}
