package cn.yxffcode.yugo.obj.http;

import cn.yxffcode.yugo.def.TableDefBuilder;
import cn.yxffcode.yugo.obj.ColumnDef;
import cn.yxffcode.yugo.obj.FieldBasedTableResultResolver;
import cn.yxffcode.yugo.obj.GenericTable;
import cn.yxffcode.yugo.obj.ObjectApiSchema;
import cn.yxffcode.yugo.obj.ObjectTableModificationRule;
import cn.yxffcode.yugo.obj.TableDef;
import cn.yxffcode.yugo.obj.TableResultResolver;
import cn.yxffcode.yugo.utils.Reflections;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.primitives.Primitives;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.commons.lang3.StringUtils;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import java.util.function.Supplier;

/** @author gaohang */
public class HttpApiSchemaFactory implements SchemaFactory {

  private static final ConcurrentMap<String, HttpApiSchema> CREATED_SCHEMAS =
      Maps.newConcurrentMap();

  private static final HttpTableDefParser tableDefParser = HttpTableDefParser.INSTANCE;

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

  /** 从json配置中解析出{@link HttpTableDef} */
  private enum HttpTableDefParser {
    INSTANCE;
    private final Map<String, Class<?>> primitiveClasses;

    HttpTableDefParser() {
      final Set<Class<?>> primitiveTypes = Primitives.allPrimitiveTypes();
      final Map<String, Class<?>> classes = Maps.newHashMapWithExpectedSize(primitiveTypes.size());
      primitiveTypes.forEach(
          clazz -> {
            classes.put(clazz.getName(), clazz);
          });
      this.primitiveClasses = Collections.unmodifiableMap(classes);
    }

    private Map<String, ? extends HttpTableDef> parse(final Map<String, Object> operand) {

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
      return new FieldBasedTableResultResolver(checker);
    }
  }

  /** 适配http接口的schema */
  private static final class HttpApiSchema extends ObjectApiSchema {

    HttpApiSchema(final String name, final Map<String, ? extends HttpTableDef> tableDefs) {
      super(name, tableDefs);
    }

    @Override
    protected Table createTable(final TableDef tableDef) {
      final HttpTableDef httpTableDef = (HttpTableDef) tableDef;
      return new HttpTable(httpTableDef, new HttpReqTableSelectInvoker(httpTableDef));
    }
  }

  /** @author gaohang */
  private static final class HttpTable extends GenericTable {

    private HttpTable(
        final HttpTableDef tableDef,
        final Function<Map<String, Object>, ? extends List<Object[]>> invoker) {
      super(tableDef, invoker);
    }

    @Override
    public HttpTableDef getTableDef() {
      return (HttpTableDef) tableDef;
    }

    protected void registerRules(RelOptPlanner planner, Convention convention) {
      planner.addRule(
          new ObjectTableModificationRule(
              getTableDef(),
              RelFactories.LOGICAL_BUILDER,
              HttpTableInsertExecutionLogic.getInstance()));
    }
  }
}
