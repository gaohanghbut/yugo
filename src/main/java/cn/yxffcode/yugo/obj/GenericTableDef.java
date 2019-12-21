package cn.yxffcode.yugo.obj;

import com.google.common.base.MoreObjects;

import java.util.List;
import java.util.Objects;

/** @author gaohang */
public class GenericTableDef implements TableDef {
  protected final String name;
  protected final List<ColumnDef> columnDefs;
  protected final TableResultResolver tableResultResolver;

  public GenericTableDef(
      final String name,
      final List<ColumnDef> columnDefs,
      final TableResultResolver tableResultResolver) {
    this.name = name;
    this.columnDefs = columnDefs;
    this.tableResultResolver = tableResultResolver;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public List<ColumnDef> getColumnDefs() {
    return columnDefs;
  }

  @Override
  public int getColumnCount() {
    return columnDefs.size();
  }

  @Override
  public String getColumnName(final int index) {
    return columnDefs.get(index).getName();
  }

  @Override
  public TableResultResolver getTableResultResolver() {
    return tableResultResolver;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    final GenericTableDef that = (GenericTableDef) o;
    return Objects.equals(getName(), that.getName())
        && Objects.equals(getColumnDefs(), that.getColumnDefs())
        && Objects.equals(getTableResultResolver(), that.getTableResultResolver());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getName(), getColumnDefs(), getTableResultResolver());
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("name", name)
        .add("columnDefs", columnDefs)
        .add("tableResultResolver", tableResultResolver)
        .toString();
  }
}
