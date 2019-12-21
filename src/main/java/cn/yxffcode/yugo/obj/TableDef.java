package cn.yxffcode.yugo.obj;

import java.util.List;

/**
 * 表定义，是一个接口，其实现类可以新增其它必要元素
 *
 * @author gaohang
 */
public interface TableDef {
  /** @return 表名 */
  String getName();

  /** @return 表的字段定义 */
  List<ColumnDef> getColumnDefs();

  /** @return 字段数量 */
  int getColumnCount();

  /**
   * 获取字段名
   *
   * @param index 字段索引，表示第几个字段
   * @return 字段名
   */
  String getColumnName(int index);

  /** @return 处理表返回的数据的对象 */
  TableResultResolver getTableResultResolver();
}
