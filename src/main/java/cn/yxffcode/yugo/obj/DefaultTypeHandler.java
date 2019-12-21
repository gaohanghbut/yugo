package cn.yxffcode.yugo.obj;

import com.google.common.collect.Maps;

import java.util.Map;
import java.util.function.Function;

/**
 * 字段的类型转换，可作为{@link ColumnDef#getTypeHandler()}
 *
 * <p>TODO:没有真正用起来
 *
 * @author gaohang
 */
class DefaultTypeHandler implements TypeHandler<Object, Object> {

  private static final DefaultTypeHandler INSTANCE = new DefaultTypeHandler();

  private Map<String, Function<Object, Object>> delegates = Maps.newHashMap();

  private DefaultTypeHandler() {
    this.delegates.put(
        "int",
        val -> {
          if (val instanceof String) {
            return Integer.parseInt((String) val);
          }
          return val;
        });
  }

  public static DefaultTypeHandler getInstance() {
    return INSTANCE;
  }

  @Override
  public Object handle(final Object input, final ColumnDef columnDef) {
    final Function<Object, Object> func = delegates.get(columnDef.getName());
    if (func == null) {
      return input;
    }
    return func.apply(input);
  }
}
