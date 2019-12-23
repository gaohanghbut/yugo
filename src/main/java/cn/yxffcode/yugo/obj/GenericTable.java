package cn.yxffcode.yugo.obj;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.java.AbstractQueryableTable;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.logical.LogicalTableModify;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.FilterableTable;
import org.apache.calcite.schema.ModifiableTable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.calcite.linq4j.Linq4j.iterableEnumerator;

/**
 * 通用化的{@link org.apache.calcite.schema.Table}的抽象类实现，需要指定表中一行的数据类型 （一般是Object数组) 和{@link
 * TableDef}表定义
 *
 * @see TableDef
 * @author gaohang
 */
public class GenericTable extends AbstractQueryableTable
    implements ModifiableTable, FilterableTable {

  /** table definition */
  protected final TableDef tableDef;
  /** called to request data. */
  protected final Function<Map<String, Object>, ? extends List<Object[]>> selectInvoker;

  /**
   * 构造器
   *
   * @param tableDef 表定义
   */
  protected GenericTable(
      final TableDef tableDef,
      final Function<Map<String, Object>, ? extends List<Object[]>> selectInvoker) {
    super(Object[].class);
    this.tableDef = checkNotNull(tableDef);
    checkArgument(!tableDef.getColumnDefs().isEmpty());
    this.selectInvoker = selectInvoker;
  }

  @Override
  public RelDataType getRowType(final RelDataTypeFactory typeFactory) {
    return createRelDataType(typeFactory);
  }

  private RelDataType createRelDataType(final RelDataTypeFactory typeFactory) {
    final List<RelDataType> typeList =
        Lists.newArrayListWithCapacity(tableDef.getColumnDefs().size());
    final List<String> fieldNameList =
        Lists.newArrayListWithCapacity(tableDef.getColumnDefs().size());
    for (ColumnDef columnDef : tableDef.getColumnDefs()) {
      final Class<?> type = columnDef.getType();
      if (type == Object.class || type == Comparable.class) {
        typeList.add(typeFactory.createSqlType(SqlTypeName.ANY));
      } else {
        typeList.add(typeFactory.createJavaType(type));
      }
      fieldNameList.add(columnDef.getName());
    }

    return typeFactory.createStructType(typeList, fieldNameList);
  }

  @Override
  public <T> Queryable<T> asQueryable(
      final QueryProvider queryProvider, final SchemaPlus schema, final String tableName) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Enumerable<Object[]> scan(final DataContext root, final List<RexNode> filters) {
    return new AbstractEnumerable<Object[]>() {
      @Override
      public Enumerator<Object[]> enumerator() {
        final Map<String, Object> filterValues = Maps.newHashMap();
        filters.forEach(filter -> addFilter(root, filter, filterValues));
        final List<Object[]> values = selectInvoker.apply(filterValues);
        return iterableEnumerator(values);
      }
    };
  }

  public TableDef getTableDef() {
    return tableDef;
  }

  protected boolean addFilter(
      final DataContext root, final RexNode filter, final Map<String, Object> filterValues) {
    if (filter.isA(SqlKind.AND)) {
      // We cannot refine(remove) the operands of AND,
      // it will cause o.a.c.i.TableScanNode.createFilterable filters check failed.
      ((RexCall) filter)
          .getOperands()
          .forEach(subFilter -> addFilter(root, subFilter, filterValues));
    } else if (filter.isA(SqlKind.EQUALS)) {
      final RexCall call = (RexCall) filter;
      RexNode left = call.getOperands().get(0);
      if (left.isA(SqlKind.CAST)) {
        left = ((RexCall) left).operands.get(0);
      }
      final RexNode right = call.getOperands().get(1);
      if (left instanceof RexInputRef) {
        final int index = ((RexInputRef) left).getIndex();
        final ColumnDef columnDef = tableDef.getColumnDefs().get(index);
        if (!filterValues.containsKey(index)) {
          Object value = null;
          if (right instanceof RexLiteral) {
            value = ((RexLiteral) right).getValue2();
          } else if (right instanceof RexDynamicParam) {
            value = root.get(((RexDynamicParam) right).getName());
          }
          filterValues.put(columnDef.getName(), value);
          return true;
        }
      }
    }
    return false;
  }

  @Override
  public Collection getModifiableCollection() {
    return null;
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

  /**
   * 向{@link RelOptPlanner}注册{@link org.apache.calcite.plan.RelOptRule}
   *
   * @param planner 优化器
   * @param convention 当前使用的convention
   */
  protected void registerRules(final RelOptPlanner planner, final Convention convention) {}
}
