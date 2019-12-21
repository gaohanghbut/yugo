package cn.yxffcode.yugo.obj;

/**
 * 类型转换接口，将对象从T转换到R
 *
 * @param <T> 输入类型
 * @param <R> 输出类型
 * @author gaohang
 */
public interface TypeHandler<T, R> {

  /**
   * 执行数据从类型T到类型R的转换
   *
   * @param input 转换的对象
   * @param columnDef 转换的对象所属的字段定义
   * @return 转换后的对象
   */
  R handle(T input, ColumnDef columnDef);
}
