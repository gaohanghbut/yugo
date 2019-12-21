package cn.yxffcode.yugo.obj.http;

import cn.yxffcode.yugo.obj.ColumnDef;
import cn.yxffcode.yugo.obj.MapBasedTableInvoker;
import cn.yxffcode.yugo.obj.ParameterIndex;
import cn.yxffcode.yugo.obj.RowFilter;
import cn.yxffcode.yugo.utils.HttpClients;
import com.alibaba.fastjson.JSON;
import com.google.common.collect.Maps;
import org.apache.calcite.DataContext;
import org.apache.calcite.interpreter.Bindables;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * insert语句的执行器，用于执行接口的调用，支持子查询
 *
 * <p>{@link #execute(String, String, DataContext)}方法是在{@link
 * org.apache.calcite.linq4j.tree.Expression}表示的代码所编译生成的对象中调用
 *
 * @see HttpTableInsertExecutionLogic
 * @author gaohang
 */
public class HttpReqTableInsertInvoker extends MapBasedTableInvoker {
  private static final Logger logger = LoggerFactory.getLogger(HttpReqTableInsertInvoker.class);

  private static final String COUNT_VALUE_KEY = "ROWCOUNT";

  private final HttpTableDef tableDef;

  /**
   * 通过请求对象的类型以及响应对象的类型创建对象
   *
   * @param tableDef 表定义
   */
  protected HttpReqTableInsertInvoker(final HttpTableDef tableDef) {
    super(tableDef);
    this.tableDef = tableDef;
  }

  /**
   * 在{@link * org.apache.calcite.linq4j.tree.Expression}表示的代码所编译生成的对象中调用
   *
   * @see HttpTableInsertExecutionLogic
   * @return 执行结果
   */
  public static Enumerable execute(
      final String schemaName, final String tableName, final DataContext context) {
    final List<ParameterIndex> indexList =
        (List<ParameterIndex>) context.get(ParameterIndex.CONTEXT_PARAMETER_KEY);
    return new AbstractEnumerable() {
      @Override
      public Enumerator enumerator() {
        int count = 0;
        for (ParameterIndex pi : indexList) {
          final Bindables.BindableTableScan subSelect = pi.getSubSelect();
          if (subSelect == null) {
            count += invokeByParameterIndex(schemaName, tableName, context, pi);
          } else {
            count += invokeBySubSelect(schemaName, tableName, context, subSelect);
          }
        }
        return Linq4j.singletonEnumerator(count);
      }
    };
  }

  private static int invokeBySubSelect(
      final String schemaName,
      final String tableName,
      final DataContext context,
      final Bindables.BindableTableScan subSelect) {
    final RowFilter rowFilter = new RowFilter(subSelect.getCluster());
    final Enumerable<Object[]> scanValues = subSelect.bind(context);
    final Enumerable<Object[]> iter = rowFilter.filter(scanValues, context, subSelect);

    final HttpTableDef tableDef = getHttpReqTableDef(schemaName, tableName);

    // invoke insert.
    int count = 0;
    for (Object[] from : iter) {
      final ParameterIndex parameterIndex = new ParameterIndex();
      final List<ColumnDef> columnDefs = tableDef.getColumnDefs();
      for (int i = 0; i < columnDefs.size() && i < from.length; i++) {
        final ColumnDef columnDef = columnDefs.get(i);
        // 这里都是具体的值，不会是？
        parameterIndex.addLast(columnDef.getName(), from[i], null);
      }
      final int insertResult =
          invokeByParameterIndex(schemaName, tableName, context, parameterIndex);
      count += (int) insertResult;
    }
    return count;
  }

  private static int invokeByParameterIndex(
      final String schemaName,
      final String tableName,
      final DataContext context,
      final ParameterIndex pi) {
    final Map<String, Object> parameterValues = getRequestParameters(pi, context);
    final HttpTableDef tableDef = getHttpReqTableDef(schemaName, tableName);
    final HttpReqTableInsertInvoker insertInvoker = new HttpReqTableInsertInvoker(tableDef);
    final List<Object[]> rows = insertInvoker.apply(parameterValues);
    return (int) rows.get(0)[0];
  }

  private static HttpTableDef getHttpReqTableDef(final String schemaName, final String tableName) {
    return HttpApiSchemaFactory.getTableDef(schemaName, tableName);
  }

  private static Map<String, Object> getRequestParameters(
      final ParameterIndex parameterIndex, final DataContext context) {
    final Map<String, Object> parameterValues = Maps.newHashMap();

    for (Map.Entry<String, Object> en : parameterIndex.parameterValues().entrySet()) {
      final Object value = en.getValue();
      final ParameterIndex.DynamicValue dynamicValue = ParameterIndex.dynamicValue();
      if (Objects.equals(dynamicValue, value) || Objects.equals(dynamicValue.name(), value)) {
        final int position = parameterIndex.position(en.getKey());
        parameterValues.put(en.getKey(), context.get("?" + position));
      } else {
        parameterValues.put(en.getKey(), en.getValue());
      }
    }
    return parameterValues;
  }

  @Override
  protected List<Map> invoke(final Map request) throws Exception {
    final HttpPost httpPost = new HttpPost(tableDef.getUrl());
    httpPost.addHeader("Content-Type", "application/json");
    final StringEntity entity = new StringEntity(JSON.toJSONString(request));
    httpPost.setEntity(entity);
    try (final CloseableHttpResponse response = HttpClients.HTTP_CLIENT.execute(httpPost)) {
      logger.info("request:{}|{}", 'Y', tableDef.getUrl());
      final String rst = EntityUtils.toString(response.getEntity());
      final Map map = JSON.parseObject(rst, Map.class);
      logger.debug(
          "request:{}|{}, params: {}, responses: {}", 'Y', tableDef.getUrl(), request, map);
      tableDef.getTableResultResolver().validate(map);
      return Collections.singletonList(Collections.singletonMap(COUNT_VALUE_KEY, 1));
    }
  }

  @Override
  protected Object[] toRowData(final Map req, final Map resp) {
    return new Object[] {resp.get(COUNT_VALUE_KEY)};
  }
}
