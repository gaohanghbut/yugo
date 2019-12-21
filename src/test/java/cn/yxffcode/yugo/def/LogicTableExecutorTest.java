package cn.yxffcode.yugo.def;

import cn.yxffcode.yugo.obj.http.HttpTableResultResolver;
import org.junit.Test;

import java.util.List;
import java.util.Map;

/** @author gaohang */
public class LogicTableExecutorTest {

  @Test
  public void testConfigFile() {
    final LogicTableExecutor logicTableExecutor = new LogicTableExecutor("httpapi");
    final List<Map<String, Object>> values =
        logicTableExecutor.select("select * from route_list where page = 1 and page_size = 100");
    System.out.println("values = " + values);
  }

  @Test
  public void testManulCodeTableConfig() {
    final HttpTableResultResolver resultResolver =
        new HttpTableResultResolver("code", 0, "data", "msg");

    SchemaDefBuilder.http()
        .table("http://localhost:8088/route/list")
        .name("route_list")
        .columns("routeName", "eventType", "eventCode", "page")
        .column("pageSize", "limit")
        .tableResultResolver(resultResolver)
        .<SchemaDefBuilder.HttpSchemaDefBuilder>register()
        .table("http://localhost:8088/test/insert")
        .name("target_table")
        .columns("routeName", "eventType", "eventCode")
        .tableResultResolver(resultResolver)
        .register()
        .build();

    final LogicTableExecutor logicTableExecutor = new LogicTableExecutor();
    final List<Map<String, Object>> values =
        logicTableExecutor.select("select * from route_list where page = 1 and pageSize = 100");
    System.out.println("values = " + values);
  }
}
