package cn.yxffcode.yugo.obj;

import org.apache.calcite.adapter.enumerable.EnumerableInterpreter;
import org.apache.calcite.interpreter.Bindables;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.List;

/**
 * 用于处理子查询
 *
 * @see TableInsertSqlConverter#visit(EnumerableInterpreter)
 * @author gaohang
 */
public final class SqlScanNode extends SqlIdentifier {

  private final Bindables.BindableTableScan tableScan;

  public SqlScanNode(
      final Bindables.BindableTableScan tableScan,
      final List<String> names,
      final SqlParserPos pos) {
    super(names, pos);
    this.tableScan = tableScan;
  }

  public Bindables.BindableTableScan getTableScan() {
    return tableScan;
  }
}
