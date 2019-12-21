package cn.yxffcode.yugo.obj.http;

import cn.yxffcode.yugo.obj.ColumnDef;
import cn.yxffcode.yugo.obj.GenericTableDef;
import cn.yxffcode.yugo.obj.TableResultResolver;
import com.google.common.base.MoreObjects;

import java.util.List;
import java.util.Objects;

/** @author gaohang */
public class HttpTableDef extends GenericTableDef {

  private final String url;

  public HttpTableDef(
      final String name,
      final List<ColumnDef> columnDefs,
      final String url,
      final TableResultResolver tableResultResolver) {
    super(name, columnDefs, tableResultResolver);
    this.url = url;
  }

  public String getUrl() {
    return url;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    final HttpTableDef that = (HttpTableDef) o;
    return Objects.equals(getName(), that.getName())
        && Objects.equals(getColumnDefs(), that.getColumnDefs())
        && Objects.equals(getUrl(), that.getUrl());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getName(), getColumnDefs(), getUrl());
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("name", name)
        .add("fieldDefs", columnDefs)
        .add("url", url)
        .toString();
  }
}
