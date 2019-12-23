package cn.yxffcode.yugo.utils;

import com.google.common.collect.Lists;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;

/** @author gaohang on 15/12/4. */
public final class Reflections {
  private Reflections() {}

  public static <T> T newInstance(
      final Class<T> type, final Class<?>[] argTypes, final Object[] args) {
    try {
      final Constructor<T> constructor = type.getDeclaredConstructor(argTypes);
      if (!constructor.isAccessible()) {
        constructor.setAccessible(true);
      }
      return constructor.newInstance(args);
    } catch (Exception ex) {
      throw new IllegalStateException(
          "Unexpected reflection exception - " + ex.getClass().getName() + ": " + ex.getMessage(),
          ex);
    }
  }

  public static Field findField(Class<?> clazz, String name) {
    return findField(clazz, name, null);
  }

  public static Field findField(Class<?> clazz, String name, Class<?> type) {
    Class<?> searchType = clazz;
    while (!Object.class.equals(searchType) && searchType != null) {
      Field[] fields = searchType.getDeclaredFields();
      for (Field field : fields) {
        if ((name == null || name.equals(field.getName()))
            && (type == null || type.equals(field.getType()))) {
          return field;
        }
      }
      searchType = searchType.getSuperclass();
    }
    return null;
  }

  public static List<Field> getFields(Class<?> clazz) {
    final List<Field> fields = Lists.newArrayList();
    Class<?> type = clazz;
    while (type != Object.class) {
      final Field[] declaredFields = type.getDeclaredFields();
      fields.addAll(Arrays.asList(declaredFields));
      type = type.getSuperclass();
    }
    return fields;
  }

  public static Object getField(String fieldName, Object target) {
    if (target == null) {
      throw new IllegalArgumentException("target object cannot be null");
    }
    Field field = findField(target.getClass(), fieldName);
    if (field == null) {
      throw new IllegalArgumentException(
          "field " + fieldName + " is not exists in type " + target.getClass().getCanonicalName());
    }
    if (!field.isAccessible()) {
      field.setAccessible(true);
    }
    try {
      return field.get(target);
    } catch (IllegalAccessException ex) {
      throw new IllegalStateException(
          "Unexpected reflection exception - " + ex.getClass().getName() + ": " + ex.getMessage(),
          ex);
    }
  }

  public static <T> T newInstance(Class<? extends T> clazz) {
    try {
      return clazz.newInstance();
    } catch (Exception e) {
      throw new RuntimeException("class initialize failed.", e);
    }
  }

  public static Class<?> forname(final String name) {
    try {
      return Class.forName(name);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }
}
