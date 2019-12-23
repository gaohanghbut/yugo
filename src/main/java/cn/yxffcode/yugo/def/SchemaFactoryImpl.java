package cn.yxffcode.yugo.def;

import cn.yxffcode.yugo.obj.TableDef;
import cn.yxffcode.yugo.obj.http.HttpApiSchemaFactory;
import cn.yxffcode.yugo.obj.http.HttpTableDef;
import cn.yxffcode.yugo.obj.local.LocalApiSchemaFactory;
import cn.yxffcode.yugo.obj.local.LocalServiceTableDef;
import com.google.common.collect.Maps;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.commons.lang3.ObjectUtils;

import java.util.Collections;
import java.util.Map;

/** @author gaohang */
public class SchemaFactoryImpl implements SchemaFactory {

  private static final Map<SchemaType, SchemaFactory> FACTORIES;

  static {
    final Map<SchemaType, SchemaFactory> factories = Maps.newHashMap();
    factories.put(
        SchemaType.HTTP,
        new HttpApiSchemaFactory(
            () -> (Map<String, HttpTableDef>) getRegisteredTableDefMap(SchemaType.HTTP)));
    factories.put(
        SchemaType.LOCAL,
        new LocalApiSchemaFactory(
            () -> (Map<String, LocalServiceTableDef>) getRegisteredTableDefMap(SchemaType.LOCAL)));
    FACTORIES = Collections.unmodifiableMap(factories);
  }

  private static Map<String, ? extends TableDef> getRegisteredTableDefMap(
      final SchemaType schemaType) {
    final Map<String, ? extends TableDef> tableDefs = SchemaDefBuilder.getTableDefs(schemaType);
    if (tableDefs == null) {
      return Collections.emptyMap();
    }
    return tableDefs;
  }

  @Override
  public Schema create(
      final SchemaPlus parentSchema, final String name, final Map<String, Object> operand) {
    for (SchemaType schemaType : SchemaType.values()) {
      if (ObjectUtils.equals(name, schemaType.name)) {
        return FACTORIES.get(schemaType).create(parentSchema, name, operand);
      }
    }
    throw new IllegalArgumentException(
        name
            + " is not supported, just suport tables represent in "
            + SchemaType.class.getCanonicalName());
  }
}
