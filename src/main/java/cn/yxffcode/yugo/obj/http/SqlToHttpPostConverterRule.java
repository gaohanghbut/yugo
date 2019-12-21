package cn.yxffcode.yugo.obj.http;

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.tools.RelBuilderFactory;

import java.util.function.Predicate;

/** @author gaohang */
class SqlToHttpPostConverterRule extends ConverterRule {

  SqlToHttpPostConverterRule(
      final HttpTableDef tableRef, Convention out, RelBuilderFactory relBuilderFactory) {
    super(
        RelNode.class,
        (Predicate<RelNode>) r -> true,
        out,
        EnumerableConvention.INSTANCE,
        relBuilderFactory,
        "SqlToHttpPostConverterRule");
  }

  @Override
  public RelNode convert(RelNode rel) {
    RelTraitSet newTraitSet = rel.getTraitSet().replace(getOutTrait());
    return new SqlToHttpPostConverter(rel.getCluster(), newTraitSet, rel);
  }
}
