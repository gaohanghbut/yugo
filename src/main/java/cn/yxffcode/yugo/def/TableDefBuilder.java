package cn.yxffcode.yugo.def;

import cn.yxffcode.yugo.obj.ColumnDef;
import cn.yxffcode.yugo.obj.FieldBasedTableResultResolver;
import cn.yxffcode.yugo.obj.TableDef;
import cn.yxffcode.yugo.obj.TableResultResolver;
import cn.yxffcode.yugo.obj.TypeHandler;
import cn.yxffcode.yugo.obj.http.HttpTableDef;
import com.google.common.collect.Lists;

import java.util.List;

/** @author gaohang */
public abstract class TableDefBuilder {
  protected String name;
  protected List<ColumnDef> columnDefs = Lists.newArrayList();
  protected TableResultResolver tableResultResolver;

  private SchemaDefBuilder schemaDefBuilder;

  private TableDefBuilder() {}

  public static final TableDefBuilder httpTable(final String url) {
    return new TableDefBuilder() {
      @Override
      public TableDef build() {
        return new HttpTableDef(name, columnDefs, url, tableResultResolver);
      }
    };
  }

  public TableDefBuilder name(final String name) {
    this.name = name;
    return this;
  }

  public TableDefBuilder columnDefs(final List<ColumnDef> columnDefs) {
    this.columnDefs.addAll(columnDefs);
    return this;
  }

  public TableDefBuilder column(
      final String name,
      final Class<?> type,
      final boolean key,
      final String mapping,
      final TypeHandler typeHandler) {
    this.columnDefs.add(new ColumnDef(name, type, mapping, key, typeHandler));
    return this;
  }

  public TableDefBuilder column(
      final String name, final Class<?> type, final boolean key, final String mapping) {
    this.columnDefs.add(new ColumnDef(name, type, mapping, key));
    return this;
  }

  public TableDefBuilder column(final String name, final Class<?> type, final String mapping) {
    this.columnDefs.add(new ColumnDef(name, type, mapping, false));
    return this;
  }

  public TableDefBuilder column(final String name, final Class<?> type, final boolean key) {
    this.columnDefs.add(new ColumnDef(name, type, name, key));
    return this;
  }

  public TableDefBuilder column(final String name, final Class<?> type) {
    return column(name, type, false);
  }

  public TableDefBuilder column(final String name, final String mapping) {
    return column(name, Comparable.class, mapping);
  }

  public TableDefBuilder column(final String name) {
    return column(name, Comparable.class);
  }

  public TableDefBuilder columns(final String... names) {
    for (String name : names) {
      column(name, Comparable.class);
    }
    return this;
  }

  public TableDefBuilder tableResultResolver(final TableResultResolver tableResultResolver) {
    this.tableResultResolver = tableResultResolver;
    return this;
  }

  public final TableDefBuilder tableResultResolver(
      final String codeKey,
      final Object successValue,
      final String dataKey,
      final String errMsgKey) {
    return tableResultResolver(
        new FieldBasedTableResultResolver(codeKey, successValue, dataKey, errMsgKey));
  }

  public abstract TableDef build();

  public <T extends SchemaDefBuilder> T register() {
    schemaDefBuilder.register(build());
    return (T) schemaDefBuilder;
  }

  void setSchemaDefBuilder(final SchemaDefBuilder schemaDefBuilder) {
    this.schemaDefBuilder = schemaDefBuilder;
  }
}
