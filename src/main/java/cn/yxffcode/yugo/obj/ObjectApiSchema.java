package cn.yxffcode.yugo.obj;

import com.google.common.collect.Maps;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

import java.util.Collections;
import java.util.Map;

/** @author gaohang */
public abstract class ObjectApiSchema extends AbstractSchema {
  protected final Map<String, Table> tableMap;
  protected final Map<String, RelProtoDataType> typeMap;
  protected final String name;

  protected ObjectApiSchema(final String name, final Map<String, ? extends TableDef> tableDefs) {
    this.name = name;
    if (tableDefs == null) {
      this.tableMap = Collections.emptyMap();
    } else {
      final Map<String, Table> tableMap = Maps.newHashMapWithExpectedSize(tableDefs.size());
      for (Map.Entry<String, ? extends TableDef> en : tableDefs.entrySet()) {
        final TableDef tableDef = en.getValue();
        tableMap.put(en.getKey(), createTable(tableDef));
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

  protected abstract Table createTable(TableDef tableDef);

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
