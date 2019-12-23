package cn.yxffcode.yugo.obj;

import com.google.common.base.MoreObjects;

import java.lang.reflect.Field;
import java.util.Objects;

/**
 * 表字段的定义，支持字段的映射（见{@link #mapping}）
 *
 * <p>TODO:{@link #typeHandler}还没有真正利用起来
 *
 * @author gaohang
 */
public final class ColumnDef {
  /** 字段名 */
  private final String name;
  /** 字段类型，默认使用{@link Comparable} */
  private final Class<?> type;
  /** 字段映射，表示字段在真实的数据源中的取值路径 比如表字段为name，数据源中为user.name. */
  private final String mapping;
  /** 该字段是否为表的key */
  private final boolean key;
  /** 用于执行类型转换，需要的数据类型为{@link #type}但在数据源中不一定是{@link #type} */
  private final TypeHandler<Object, Object> typeHandler;

  public ColumnDef(Field field) {
    this(field, false);
  }

  public ColumnDef(Field field, boolean key) {
    this(field.getName(), field.getType(), field.getName(), key);
  }

  public ColumnDef(final String name, final Class<?> type) {
    this(name, type, name, false, DefaultTypeHandler.getInstance());
  }

  public ColumnDef(final String name, final Class<?> type, final boolean key) {
    this(name, type, name, key, DefaultTypeHandler.getInstance());
  }

  public ColumnDef(
      final String name, final Class<?> type, final String mapping, final boolean key) {
    this(name, type, mapping, key, DefaultTypeHandler.getInstance());
  }

  public ColumnDef(
      final String name,
      final Class<?> type,
      final String mapping,
      final boolean key,
      final TypeHandler<Object, Object> typeHandler) {
    this.name = name;
    this.type = type;
    this.mapping = mapping;
    this.key = key;
    this.typeHandler = typeHandler;
  }

  public TypeHandler<Object, Object> getTypeHandler() {
    return typeHandler;
  }

  public String getName() {
    return name;
  }

  public Class<?> getType() {
    return type;
  }

  public String getMapping() {
    return mapping;
  }

  public boolean isKey() {
    return key;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    final ColumnDef columnDef = (ColumnDef) o;
    return isKey() == columnDef.isKey()
        && Objects.equals(getName(), columnDef.getName())
        && Objects.equals(getType(), columnDef.getType())
        && Objects.equals(getMapping(), columnDef.getMapping());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getName(), getType(), getMapping(), isKey());
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("name", name)
        .add("type", type)
        .add("mapping", mapping)
        .add("key", key)
        .toString();
  }
}
