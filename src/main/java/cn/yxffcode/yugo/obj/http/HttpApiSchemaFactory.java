package cn.yxffcode.yugo.obj.http;

import cn.yxffcode.yugo.obj.TableDefParser;
import com.google.common.collect.Maps;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Supplier;

/** @author gaohang */
public class HttpApiSchemaFactory implements SchemaFactory {

  private static final ConcurrentMap<String, HttpApiSchema> CREATED_SCHEMAS =
      Maps.newConcurrentMap();

  private static final TableDefParser<HttpTableDef> tableDefParser = new HttpTableDefParser();

  private final Supplier<Map<String, HttpTableDef>> tableDefSupplier;

  public HttpApiSchemaFactory() {
    this(() -> Collections.emptyMap());
  }

  public HttpApiSchemaFactory(final Supplier<Map<String, HttpTableDef>> tableDefSupplier) {
    this.tableDefSupplier = tableDefSupplier;
  }

  public static final HttpApiSchema getSchema(final String schemaName) {
    return CREATED_SCHEMAS.get(schemaName);
  }

  public static final HttpTableDef getTableDef(final String schemaName, final String tableName) {
    final HttpApiSchema schema = getSchema(schemaName);
    if (schema == null) {
      throw new IllegalArgumentException("schema [" + schemaName + "] is not exists");
    }
    final HttpTable table = (HttpTable) schema.getTable(tableName);
    if (table == null) {
      throw new IllegalArgumentException(
          "table [" + schemaName + '.' + tableName + "] is not exists");
    }
    return table.getTableDef();
  }

  @Override
  public Schema create(
      final SchemaPlus parentSchema, final String name, final Map<String, Object> operand) {
    return CREATED_SCHEMAS.computeIfAbsent(
        name,
        __ -> {
          final Map<String, HttpTableDef> tableDefs = Maps.newHashMap();
          tableDefs.putAll(tableDefParser.parse(operand));
          tableDefs.putAll(tableDefSupplier.get());
          return new HttpApiSchema(name, tableDefs);
        });
  }
}
