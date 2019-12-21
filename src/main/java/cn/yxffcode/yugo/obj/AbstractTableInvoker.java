package cn.yxffcode.yugo.obj;

import cn.yxffcode.yugo.utils.Reflections;
import org.springframework.beans.BeanWrapper;
import org.springframework.beans.BeanWrapperImpl;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * 执行请求获取数据的抽象实现
 *
 * @param <Req> request对象的类型
 * @param <Resp> response对象的类型
 * @author gaohang
 */
public abstract class AbstractTableInvoker<Req, Resp>
    implements Function<Map<String, Object>, List<Object[]>> {
  /** 请求对象的类型 */
  private final Class<? extends Req> requestClass;
  /** 数据对象的类型 */
  private final Class<? extends Resp> responseClass;

  private final TableDef tableDef;

  /**
   * 通过请求对象的类型以及响应对象的类型创建对象
   *
   * @param requestClass 请求对象的类型
   * @param responseClass 响应结果的类型
   * @param tableDef 表定义
   */
  protected AbstractTableInvoker(
      final Class<Req> requestClass, final Class<Resp> responseClass, final TableDef tableDef) {
    this.requestClass = requestClass;
    this.responseClass = responseClass;
    this.tableDef = tableDef;
  }

  @Override
  public List<Object[]> apply(final Map<String, Object> params) {
    final Req req = createRequest(params);
    try {
      final List<Resp> resps = invoke(req);
      if (resps == null || resps.isEmpty()) {
        return Collections.emptyList();
      }
      return resps.stream().map(resp -> toRowData(req, resp)).collect(Collectors.toList());
    } catch (Exception e) {
      throw new TableExecuteException("invoke api failed", e);
    }
  }

  /**
   * 创建request对象
   *
   * @param params 请求参数
   * @return 请求对象
   */
  protected Req createRequest(final Map<String, Object> params) {
    final Req req = Reflections.newInstance(requestClass);
    final BeanWrapper beanWrapper = new BeanWrapperImpl(req);
    beanWrapper.setAutoGrowNestedPaths(true);
    tableDef
        .getColumnDefs()
        .forEach(
            fieldDef -> {
              if (!fieldDef.isKey()) {
                return;
              }
              final Object value = params.get(fieldDef.getName());
              if (value != null) {
                beanWrapper.setPropertyValue(fieldDef.getMapping(), value);
              }
            });
    return req;
  }

  protected Object[] toRowData(final Req req, final Resp resp) {
    final BeanWrapper reqBean = new BeanWrapperImpl(req);
    final BeanWrapper respBean = new BeanWrapperImpl(resp);
    final Object[] row = new Object[tableDef.getColumnDefs().size()];
    for (int i = 0; i < tableDef.getColumnDefs().size(); i++) {
      final ColumnDef columnDef = tableDef.getColumnDefs().get(i);
      // 以rep为主，如果rep中没有，则使用req中的数据
      if (columnDef.isKey()) {
        row[i] = reqBean.getPropertyValue(columnDef.getMapping());
      } else {
        row[i] = respBean.getPropertyValue(columnDef.getMapping());
      }
    }
    return row;
  }

  /**
   * request for data
   *
   * @param request request object
   * @return response data.
   */
  protected abstract List<Resp> invoke(final Req request) throws Exception;

  /** @return field definitions. */
  public List<ColumnDef> fieldDefs() {
    return tableDef.getColumnDefs();
  }

}
