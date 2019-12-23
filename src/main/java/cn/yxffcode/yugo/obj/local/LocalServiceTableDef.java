package cn.yxffcode.yugo.obj.local;

import cn.yxffcode.yugo.obj.ColumnDef;
import cn.yxffcode.yugo.obj.GenericTableDef;
import cn.yxffcode.yugo.obj.TableDefException;
import cn.yxffcode.yugo.obj.TableResultResolver;
import cn.yxffcode.yugo.utils.Reflections;
import com.google.common.collect.Lists;
import com.google.common.primitives.Primitives;
import net.sf.cglib.reflect.FastClass;
import net.sf.cglib.reflect.FastMethod;
import org.apache.commons.collections.CollectionUtils;

import java.lang.reflect.Field;
import java.util.List;

/**
 * 定义本地服务调用表
 *
 * @author gaohang
 */
public class LocalServiceTableDef extends GenericTableDef {

  /** 触发服务方法调用的对象 */
  private final Invokeable invokeable;

  private LocalServiceTableDef(
      final String name,
      final List<ColumnDef> columnDefs,
      final TableResultResolver tableResultResolver,
      final Object service,
      final FastMethod method) {
    super(name, columnDefs, tableResultResolver);
    this.invokeable = args -> method.invoke(service, args);
  }

  public static LocalServiceTableDef create(
      final String name,
      final TableResultResolver tableResultResolver,
      final Object service,
      final List<String> keyNames,
      final String method,
      final Class<?>... argTypes) {
    final FastMethod fastMethod = FastClass.create(service.getClass()).getMethod(method, argTypes);
    if (keyNames == null && argTypes.length != 0) {
      // 配置错误
      throw new TableDefException("keyNames.size() must equals argTypes.length");
    }
    final List<ColumnDef> columnDefs = Lists.newArrayList();
    if (CollectionUtils.isNotEmpty(keyNames)) {
      for (int i = 0; i < keyNames.size(); i++) {
        final String keyName = keyNames.get(i);
        columnDefs.add(new ColumnDef(keyName, argTypes[i], true));
      }
    }
    // build column definitions.
    final Class returnType = fastMethod.getReturnType();
    if (void.class == returnType || Void.class == returnType) {
      buildColumns(returnType, columnDefs);
    }

    return new LocalServiceTableDef(name, columnDefs, tableResultResolver, service, fastMethod);
  }

  private static void buildColumns(final Class returnType, final List<ColumnDef> columnDefs) {
    columnDefs.add(new ColumnDef("RETURN", returnType));
    if (Primitives.allPrimitiveTypes().contains(returnType)
        || Primitives.allWrapperTypes().contains(returnType)
        || CharSequence.class.isAssignableFrom(returnType)) {
      return;
    }
    final List<Field> fields = Reflections.getFields(returnType);
    if (CollectionUtils.isNotEmpty(fields)) {
      fields.forEach(
          field -> {
            columnDefs.add(new ColumnDef(field));
          });
    }
  }

  public Invokeable getInvokeable() {
    return invokeable;
  }
}
