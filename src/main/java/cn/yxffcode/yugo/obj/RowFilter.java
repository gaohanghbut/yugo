package cn.yxffcode.yugo.obj;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.DataContext;
import org.apache.calcite.interpreter.Bindables;
import org.apache.calcite.interpreter.Context;
import org.apache.calcite.interpreter.JaninoRexCompiler;
import org.apache.calcite.interpreter.Scalar;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;

import static org.apache.calcite.interpreter.ContextFactory.createContext;
import static org.apache.calcite.rex.RexUtil.composeConjunction;

/**
 * 建立在SQL的过滤条件之上的数据过滤，一行数据是一个Object数组
 *
 * <p>即过滤where条件，一般场景下用不上，但是由于目前calcite的adapter相关接口对insert into $table1 select * from $table2
 * 这样的sql支持非常弱，这里提供一个类执行过滤逻辑，如果不想通过手动调用{@link org.apache.calcite.sql.parser.SqlParser}
 * 解析SQL生成执行计划，则可以在处理insert into select语句时使用此类执行where条件过滤
 *
 * @author gaohang
 */
public class RowFilter {

  /** 用于生成过滤条件 */
  private final JaninoRexCompiler compiler;

  public RowFilter(final RelOptCluster cluster) {
    compiler = new JaninoRexCompiler(cluster.getRexBuilder());
  }

  /**
   * 对输入数据做过滤，返回过滤后的数据，这是一个lazy的实现
   *
   * @param rows 输入的数据
   * @param root 运行时的{@link DataContext}数据上下文
   * @param rel 表扫描语句
   * @return 过滤后的结果
   */
  public Enumerable<Object[]> filter(
      final Enumerable<Object[]> rows,
      final DataContext root,
      final Bindables.BindableTableScan rel) {
    final RexNode sqlFilter = getSQLFilter(rel);
    final RelDataType rowType = rel.getRowType();
    final Scalar condition = compiler.compile(ImmutableList.of(sqlFilter), rowType);
    final Context context = createContext(root);
    return rows.where(
        row -> {
          context.values = row;
          Boolean b = (Boolean) condition.execute(context);
          return b != null && b;
        });
  }

  private RexNode getSQLFilter(final Bindables.BindableTableScan subSelect) {
    return composeConjunction(subSelect.getCluster().getRexBuilder(), subSelect.filters);
  }
}
