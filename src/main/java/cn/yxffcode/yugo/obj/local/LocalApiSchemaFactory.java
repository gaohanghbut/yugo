package cn.yxffcode.yugo.obj.local;

import cn.yxffcode.yugo.obj.GenericTable;
import cn.yxffcode.yugo.obj.ObjectApiSchema;
import cn.yxffcode.yugo.obj.ObjectTableModificationRule;
import cn.yxffcode.yugo.obj.TableDef;
import com.google.common.collect.Maps;
import com.google.common.primitives.Primitives;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import java.util.function.Supplier;

/** @author gaohang */
public class LocalApiSchemaFactory implements SchemaFactory {

  private static final ConcurrentMap<String, LocalServiceApiSchema> CREATED_SCHEMAS =
      Maps.newConcurrentMap();

  private final Supplier<Map<String, LocalServiceTableDef>> tableDefSupplier;

  public LocalApiSchemaFactory() {
    this(() -> Collections.emptyMap());
  }

  public LocalApiSchemaFactory(final Supplier<Map<String, LocalServiceTableDef>> tableDefSupplier) {
    this.tableDefSupplier = tableDefSupplier;
  }

  public static final LocalServiceApiSchema getSchema(final String schemaName) {
    return CREATED_SCHEMAS.get(schemaName);
  }

  public static final LocalServiceTableDef getTableDef(
      final String schemaName, final String tableName) {
    final LocalServiceApiSchema schema = getSchema(schemaName);
    if (schema == null) {
      throw new IllegalArgumentException("schema [" + schemaName + "] is not exists");
    }
    final LocalServiceTable table = (LocalServiceTable) schema.getTable(tableName);
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
          final Map<String, LocalServiceTableDef> tableDefs = Maps.newHashMap();
          tableDefs.putAll(TableDefParser.INSTANCE.parse(operand));
          tableDefs.putAll(tableDefSupplier.get());
          return new LocalServiceApiSchema(name, tableDefs);
        });
  }

  /** 从json配置中解析出{@link LocalServiceTableDef} */
  private enum TableDefParser {
    INSTANCE;
    private final Map<String, Class<?>> primitiveClasses;

    TableDefParser() {
      final Set<Class<?>> primitiveTypes = Primitives.allPrimitiveTypes();
      final Map<String, Class<?>> classes = Maps.newHashMapWithExpectedSize(primitiveTypes.size());
      primitiveTypes.forEach(
          clazz -> {
            classes.put(clazz.getName(), clazz);
          });
      this.primitiveClasses = Collections.unmodifiableMap(classes);
    }

    private Map<String, ? extends LocalServiceTableDef> parse(final Map<String, Object> operand) {

      final List<Map<String, Object>> tables = (List<Map<String, Object>>) operand.get("tables");
      final Map<String, LocalServiceTableDef> tableDefs = Maps.newHashMap();
      for (Map<String, Object> table : tables) {}
      return tableDefs;
    }
  }

  /** 适配http接口的schema */
  private static final class LocalServiceApiSchema extends ObjectApiSchema {

    LocalServiceApiSchema(
        final String name, final Map<String, ? extends LocalServiceTableDef> tableDefs) {
      super(name, tableDefs);
    }

    @Override
    protected Table createTable(final TableDef tableDef) {
      final LocalServiceTableDef lstd = (LocalServiceTableDef) tableDef;
      return new LocalServiceTable(lstd, new LocalServiceTableSelectInvoker(lstd));
    }
  }

  /** @author gaohang */
  private static final class LocalServiceTable extends GenericTable {

    private LocalServiceTable(
        final LocalServiceTableDef tableDef,
        final Function<Map<String, Object>, ? extends List<Object[]>> invoker) {
      super(tableDef, invoker);
    }

    @Override
    public LocalServiceTableDef getTableDef() {
      return (LocalServiceTableDef) tableDef;
    }

    protected void registerRules(RelOptPlanner planner, Convention convention) {
      planner.addRule(
          new ObjectTableModificationRule(
              getTableDef(),
              RelFactories.LOGICAL_BUILDER,
              LocalServiceTableInsertExecutionLogic.getInstance()));
    }
  }
}
