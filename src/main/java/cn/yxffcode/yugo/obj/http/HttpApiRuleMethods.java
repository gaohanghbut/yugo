package cn.yxffcode.yugo.obj.http;

import cn.yxffcode.yugo.obj.ObjectTableModify;
import com.google.common.collect.ImmutableMap;
import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.enumerable.EnumerableRelImplementor;
import org.apache.calcite.linq4j.tree.Types;
import org.apache.calcite.util.BuiltInMethod;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;

/**
 * 在{@link HttpTableInsertExecutionLogic#implement(ObjectTableModify, java.util.List, EnumerableRelImplementor, EnumerableRel.Prefer)} 中生成代码时可使用的方法调用
 *
 * @see HttpTableInsertExecutionLogic#implement(ObjectTableModify, java.util.List, EnumerableRelImplementor, EnumerableRel.Prefer)
 * @author gaohang
 */
enum HttpApiRuleMethods {
  HTTP_TABLE_INSERT(
      HttpReqTableInsertInvoker.class, "execute", String.class, String.class, DataContext.class);
  public static final ImmutableMap<Method, BuiltInMethod> MAP;

  static {
    final ImmutableMap.Builder<Method, BuiltInMethod> builder = ImmutableMap.builder();
    for (BuiltInMethod value : BuiltInMethod.values()) {
      if (value.method != null) {
        builder.put(value.method, value);
      }
    }
    MAP = builder.build();
  }

  final Method method;
  final Constructor constructor;
  final Field field;

  HttpApiRuleMethods(Method method, Constructor constructor, Field field) {
    this.method = method;
    this.constructor = constructor;
    this.field = field;
  }

  /** Defines a method. */
  HttpApiRuleMethods(Class clazz, String methodName, Class... argumentTypes) {
    this(Types.lookupMethod(clazz, methodName, argumentTypes), null, null);
  }

  public String getMethodName() {
    return method.getName();
  }
}
