package cn.yxffcode.yugo.obj;

import cn.yxffcode.yugo.utils.Reflections;
import com.google.common.base.Splitter;
import com.google.common.collect.Maps;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;

/**
 * 基于{@link Map}的表执行器，其参数和返回结果都是{@link Map}
 *
 * @author gaohang
 */
public abstract class MapBasedTableInvoker extends AbstractTableInvoker<Map, Map> {

  private static final Splitter TREE_PATH_SPLITTER =
      Splitter.on('.').trimResults().omitEmptyStrings();
  /**
   * 通过请求对象的类型以及响应对象的类型创建对象
   *
   * @param tableDef 表定义
   */
  protected MapBasedTableInvoker(final TableDef tableDef) {
    super(Map.class, Map.class, tableDef);
  }

  @Override
  @SuppressWarnings("unchecked")
  protected Map<String, Object> createRequest(final Map<String, Object> params) {
    final Map<String, Object> request = Maps.newHashMapWithExpectedSize(params.size());
    // 根据fieldRefs中的mapping拼装成有层级结构的map
    fieldDefs()
        .forEach(
            fieldDef -> {
              final Object val = params.get(fieldDef.getName());
              if (val == null) {
                return;
              }
              final List<String> props = TREE_PATH_SPLITTER.splitToList(fieldDef.getMapping());

              Map<String, Object> data = request;
              for (int i = 0, j = props.size() - 1; i < j; i++) {
                final String key = props.get(i);
                data = (Map<String, Object>) data.computeIfAbsent(key, k -> Maps.newHashMap());
              }

              data.put(props.get(props.size() - 1), val);
            });
    return request;
  }

  @Override
  @SuppressWarnings("unchecked")
  protected Object[] toRowData(final Map req, final Map resp) {
    // 合并两个map，以response为主
    final Object[] row = new Object[fieldDefs().size()];
    for (int i = 0; i < fieldDefs().size(); i++) {
      final ColumnDef columnDef = fieldDefs().get(i);
      if (columnDef.isKey()) {
        Object value = getValue(req, columnDef);
        if (value == null) {
          value = getValue(resp, columnDef);
        }
        row[i] = value;
      } else {
        Object value = getValue(resp, columnDef);
        if (value == null) {
          value = getValue(req, columnDef);
        }
        row[i] = value;
      }
    }
    return row;
  }

  /**
   * 从对象上取值，支持对象图的访问和Map的访问
   *
   * @param data
   * @param columnDef
   * @return
   */
  private Object getValue(final Map<String, Object> data, final ColumnDef columnDef) {
    Object obj = data;
    for (String key : TREE_PATH_SPLITTER.split(columnDef.getMapping())) {
      if (obj instanceof Map) {
        obj = ((Map) obj).get(key);
      } else {
        final Field field = Reflections.findField(obj.getClass(), key);
        if (field == null) {
          // 字段不存在
          return null;
        }
        obj = Reflections.getField(key, obj);
      }
    }
    return columnDef.getTypeHandler().handle(obj, columnDef);
  }
}
