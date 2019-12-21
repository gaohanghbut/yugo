package cn.yxffcode.yugo.obj.http;

import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterImpl;

import java.util.List;

/**
 * Relational expression representing a http post api.
 *
 * @author gaohang
 */
class SqlToHttpPostConverter extends ConverterImpl {

  protected SqlToHttpPostConverter(RelOptCluster cluster, RelTraitSet traits, RelNode input) {
    super(cluster, ConventionTraitDef.INSTANCE, traits, input);
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new SqlToHttpPostConverter(getCluster(), traitSet, sole(inputs));
  }
}
