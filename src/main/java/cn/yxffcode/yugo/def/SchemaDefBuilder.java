package cn.yxffcode.yugo.def;

import cn.yxffcode.yugo.obj.TableDef;
import cn.yxffcode.yugo.obj.http.HttpTableDef;
import com.google.common.collect.Maps;

import java.util.Map;

/** @author gaohang */
public abstract class SchemaDefBuilder<T extends TableDef> {
  private static final Map<SchemaType, Map<String, ? extends TableDef>> TABLE_DEF_MAPPING =
      Maps.newEnumMap(SchemaType.class);

  protected final SchemaType schemaType;
  protected final Map<String, T> tableDefs = Maps.newHashMap();

  private SchemaDefBuilder(final SchemaType schemaType) {
    this.schemaType = schemaType;
  }

  public static Map<String, ? extends TableDef> getTableDefs(final SchemaType schemaType) {
    return TABLE_DEF_MAPPING.get(schemaType);
  }

  public static HttpSchemaDefBuilder http() {
    return new HttpSchemaDefBuilder(SchemaType.HTTP);
  }

  void register(final T tableDef) {
    tableDefs.put(tableDef.getName(), tableDef);
  }

  public final void build() {
    TABLE_DEF_MAPPING.put(schemaType, tableDefs);
  }

  public static final class HttpSchemaDefBuilder extends SchemaDefBuilder<HttpTableDef> {

    private HttpSchemaDefBuilder(final SchemaType schemaType) {
      super(schemaType);
    }

    public TableDefBuilder table(final String url) {
      final TableDefBuilder tableDefBuilder = TableDefBuilder.httpTable(url);
      tableDefBuilder.setSchemaDefBuilder(this);
      return tableDefBuilder;
    }
  }
}
