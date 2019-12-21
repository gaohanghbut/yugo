package cn.yxffcode.yugo.obj.http;

import cn.yxffcode.yugo.obj.GenericTable;
import com.google.common.collect.Maps;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.logical.LogicalTableModify;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.FilterableTable;
import org.apache.calcite.schema.ModifiableTable;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.apache.calcite.linq4j.Linq4j.iterableEnumerator;

/** @author gaohang */
class HttpTable extends GenericTable implements FilterableTable, ModifiableTable {

  /** called to request data. */
  private final Function<Map<String, Object>, ? extends List<Object[]>> invoker;

  protected HttpTable(
      final HttpTableDef tableDef,
      final Function<Map<String, Object>, ? extends List<Object[]>> invoker) {
    super(tableDef);
    this.invoker = invoker;
  }

  @Override
  public Enumerable<Object[]> scan(final DataContext root, final List<RexNode> filters) {
    return new AbstractEnumerable<Object[]>() {
      @Override
      public Enumerator<Object[]> enumerator() {
        final Map<String, Object> filterValues = Maps.newHashMap();
        filters.forEach(filter -> addFilter(root, filter, filterValues));
        final List<Object[]> values = invoker.apply(filterValues);
        return iterableEnumerator(values);
      }
    };
  }

  @Override
  public HttpTableDef getTableDef() {
    return (HttpTableDef) tableDef;
  }

  @Override
  public TableModify toModificationRel(
      final RelOptCluster cluster,
      final RelOptTable table,
      final Prepare.CatalogReader catalogReader,
      final RelNode input,
      final TableModify.Operation operation,
      final List<String> updateColumnList,
      final List<RexNode> sourceExpressionList,
      final boolean flattened) {
    final LogicalTableModify logicalTableModify =
        new LogicalTableModify(
            cluster,
            cluster.traitSetOf(Convention.NONE),
            table,
            catalogReader,
            input,
            operation,
            updateColumnList,
            sourceExpressionList,
            flattened);
    registerRules(cluster.getPlanner(), logicalTableModify.getConvention());
    return logicalTableModify;
  }

  protected void registerRules(RelOptPlanner planner, Convention convention) {
    planner.addRule(
        new SqlToHttpPostConverterRule(getTableDef(), convention, RelFactories.LOGICAL_BUILDER));
    planner.addRule(new HttpTableModificationRule(getTableDef(), RelFactories.LOGICAL_BUILDER));
  }
}
