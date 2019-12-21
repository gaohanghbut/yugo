package cn.yxffcode.yugo.obj.http;

import cn.yxffcode.yugo.obj.MapBasedTableInvoker;
import cn.yxffcode.yugo.utils.HttpClients;
import com.alibaba.fastjson.JSON;
import com.google.common.base.Joiner;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * select语句的执行器，执行接口的调用
 *
 * @author gaohang
 */
class HttpReqTableSelectInvoker extends MapBasedTableInvoker {

  private static final Logger logger = LoggerFactory.getLogger(HttpReqTableSelectInvoker.class);

  private static final Joiner.MapJoiner QUERY_STR_JOINER =
      Joiner.on('&').withKeyValueSeparator('=');

  private final HttpTableDef tableDef;

  HttpReqTableSelectInvoker(final HttpTableDef tableDef) {
    super(tableDef);
    this.tableDef = tableDef;
  }

  @Override
  protected List<Map> invoke(final Map request) {
    // transform request to query string
    final String queryStr = QUERY_STR_JOINER.join(request);
    final String requestUrl = tableDef.getUrl() + '?' + queryStr;
    final HttpGet httpGet = new HttpGet(requestUrl);
    try (final CloseableHttpResponse response = HttpClients.HTTP_CLIENT.execute(httpGet)) {
      logger.info("request:{}|{}", 'Y', requestUrl);
      final String jsonStr = EntityUtils.toString(response.getEntity());
      final Map map = JSON.parseObject(jsonStr, Map.class);
      logger.debug(
          "request:{}|{}, params: {}, responses: {}", 'Y', tableDef.getUrl(), request, map);
      return getData(map);
    } catch (IOException e) {
      throw new HttpApiInvokeException("invoke " + requestUrl + " failed", e);
    }
  }

  private List<Map> getData(final Map map) {
    final Object obj = tableDef.getTableResultResolver().data(map);
    if (obj instanceof List) {
      return (List<Map>) obj;
    }
    return Collections.singletonList((Map<String, Object>) obj);
  }
}
