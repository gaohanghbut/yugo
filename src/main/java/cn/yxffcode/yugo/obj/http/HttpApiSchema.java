package cn.yxffcode.yugo.obj.http;

import com.google.common.collect.Maps;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

import java.util.Collections;
import java.util.Map;

/**
 * 适配http接口的schema
 *
 * @author gaohang
 */
public final class HttpApiSchema extends AbstractSchema {
  private final Map<String, Table> tableMap;
  private final Map<String, RelProtoDataType> typeMap;
  private final String name;

  public HttpApiSchema(final String name, final Map<String, ? extends HttpTableDef> tableDefs) {
    this.name = name;
    if (tableDefs == null) {
      this.tableMap = Collections.emptyMap();
    } else {
      final Map<String, Table> tableMap = Maps.newHashMapWithExpectedSize(tableDefs.size());
      for (Map.Entry<String, ? extends HttpTableDef> en : tableDefs.entrySet()) {
        final HttpTableDef tableDef = en.getValue();
        tableMap.put(en.getKey(), new HttpTable(tableDef, new HttpReqTableSelectInvoker(tableDef)));
      }
      this.tableMap = Collections.unmodifiableMap(tableMap);
    }

    final Map<String, RelProtoDataType> typeMap =
        Maps.newHashMapWithExpectedSize(this.tableMap.size());
    for (Map.Entry<String, Table> en : tableMap.entrySet()) {
      typeMap.put(en.getKey(), factory -> en.getValue().getRowType(factory));
    }
    this.typeMap = Collections.unmodifiableMap(typeMap);
  }

  @Override
  protected Map<String, Table> getTableMap() {
    return tableMap;
  }

  @Override
  protected Map<String, RelProtoDataType> getTypeMap() {
    return typeMap;
  }

  public String getName() {
    return name;
  }
}
