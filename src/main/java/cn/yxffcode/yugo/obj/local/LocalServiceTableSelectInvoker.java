package cn.yxffcode.yugo.obj.local;

import cn.yxffcode.yugo.obj.MapBasedTableInvoker;
import com.google.common.base.Joiner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * select语句的执行器，执行本地的调用
 *
 * @author gaohang
 */
class LocalServiceTableSelectInvoker extends MapBasedTableInvoker {

  private static final Logger logger =
      LoggerFactory.getLogger(LocalServiceTableSelectInvoker.class);

  private static final Joiner.MapJoiner QUERY_STR_JOINER =
      Joiner.on('&').withKeyValueSeparator('=');

  private final LocalServiceTableDef tableDef;

  LocalServiceTableSelectInvoker(final LocalServiceTableDef tableDef) {
    super(tableDef);
    this.tableDef = tableDef;
  }

  @Override
  protected List<Map> invoke(final Map request) {
    return Collections.emptyList();
  }
}
