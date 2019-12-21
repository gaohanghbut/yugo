package cn.yxffcode.yugo.obj;

/**
 * 用于对表的底层数据源的返回做校验以及数据的获取
 *
 * @author gaohang
 */
public interface TableResultResolver {

  /**
   * 对底层数据源的数据做校验
   *
   * @param result 数据源的返回数据
   */
  void validate(final Object result);

  /**
   * 获取最终的数据
   *
   * @param result 数据源的返回
   * @return 最终的数据
   */
  Object data(final Object result);
}
