package cn.yxffcode.yugo.obj;

import java.util.Map;

/**
 * 表定义配置的解析接口
 *
 * @author gaohang
 */
public interface TableDefParser<T extends TableDef> {
  /**
   * 解析配置
   *
   * @param operand json配置中的operand属性
   * @return 配置解析出的表定义
   */
  Map<String, ? extends T> parse(final Map<String, Object> operand);
}
