package cn.yxffcode.yugo.obj.http;

import cn.yxffcode.yugo.def.TableDefBuilder;
import cn.yxffcode.yugo.obj.ColumnDef;
import cn.yxffcode.yugo.obj.TableDefParser;
import cn.yxffcode.yugo.obj.TableResultResolver;
import cn.yxffcode.yugo.utils.Reflections;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.primitives.Primitives;
import net.sf.cglib.beans.BeanMap;
import org.apache.commons.lang3.StringUtils;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * 从json配置中解析出{@link HttpTableDef}
 *
 * @author gaohang
 */
public class HttpTableDefParser implements TableDefParser<HttpTableDef> {
  private final Map<String, Class<?>> primitiveClasses;

  public HttpTableDefParser() {
    final Set<Class<?>> primitiveTypes = Primitives.allPrimitiveTypes();
    final Map<String, Class<?>> classes = Maps.newHashMapWithExpectedSize(primitiveTypes.size());
    primitiveTypes.forEach(
        clazz -> {
          classes.put(clazz.getName(), clazz);
        });
    this.primitiveClasses = Collections.unmodifiableMap(classes);
  }

  @Override
  public Map<String, ? extends HttpTableDef> parse(final Map<String, Object> operand) {

    final List<Map<String, Object>> tables = (List<Map<String, Object>>) operand.get("tables");
    final TableResultResolver defaultTRR =
        parseChecker((Map<String, Object>) operand.get("checker"));
    final Map<String, HttpTableDef> tableDefs = Maps.newHashMap();
    for (Map<String, Object> table : tables) {
      final List<ColumnDef> columnDefs = Lists.newArrayList();
      final List<Object> columns = (List<Object>) table.get("columns");
      for (Object rawColumn : columns) {
        if (rawColumn instanceof Map) {
          parseDetailDefinition(columnDefs, (Map<String, Object>) rawColumn);
        } else if (rawColumn instanceof List) {
          parseSimpleDefinition(columnDefs, (List<String>) rawColumn);
        }
      }
      final TableResultResolver ttr = parseTableResultResolver(defaultTRR, table);
      final HttpTableDef tableDef =
          (HttpTableDef)
              TableDefBuilder.httpTable((String) table.get("url"))
                  .name((String) table.get("name"))
                  .columnDefs(columnDefs)
                  .tableResultResolver(ttr)
                  .build();
      tableDefs.put(tableDef.getName(), tableDef);
    }
    return tableDefs;
  }

  private TableResultResolver parseTableResultResolver(
      final TableResultResolver defaultTRR, final Map<String, Object> table) {
    final Map<String, Object> checker = (Map<String, Object>) table.get("checker");
    return checker == null ? defaultTRR : parseChecker(checker);
  }

  private void parseSimpleDefinition(
      final List<ColumnDef> columnDefs, final List<String> rawColumn) {
    final List<String> columnNames = rawColumn;
    columnNames.forEach(
        name -> {
          columnDefs.add(new ColumnDef(name, Comparable.class, name, false));
        });
  }

  private void parseDetailDefinition(
      final List<ColumnDef> columnDefs, final Map<String, Object> rawColumn) {
    final Map<String, Object> column = rawColumn;
    final String columnName = (String) column.get("name");
    if (StringUtils.isNotBlank(columnName)) {
      String mapping = (String) column.get("mapping");
      if (mapping == null) {
        mapping = columnName;
      }
      final Boolean key = (Boolean) column.get("key");
      Class<?> clazz = getType(column);
      columnDefs.add(new ColumnDef(columnName, clazz, mapping, key == null ? false : key));
    }
  }

  private Class<?> getType(final Map<String, Object> column) {
    final String type = (String) column.get("type");
    Class<?> clazz = Comparable.class;
    if (type != null) {
      final Class<?> primitiveType = primitiveClasses.get(type);
      clazz = primitiveType == null ? Reflections.forname(type) : primitiveType;
    }
    return clazz;
  }

  private TableResultResolver parseChecker(final Map<String, Object> checker) {
    return new HttpTableResultResolver(checker);
  }
}
