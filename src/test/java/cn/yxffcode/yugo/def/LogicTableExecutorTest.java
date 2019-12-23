package cn.yxffcode.yugo.def;

import cn.yxffcode.yugo.obj.FieldBasedTableResultResolver;
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

    final int insertCount =
        logicTableExecutor.insert("insert into target_table values('test1', 'test2', 'test3')");
    System.out.println("insertCount = " + insertCount);

    final int batchCount =
        logicTableExecutor.insert(
            "insert into target_table values('test1', 'test2', 'test3'),('test4', 'test5', 'test5')");
    System.out.println("batchCount = " + batchCount);

    final List<Map<String, Object>> pvalues =
        logicTableExecutor.select(
            "select * from route_list where page = ? and page_size = ?", 1, 100);
    System.out.println("pvalues = " + pvalues);

    final int pinsertCount =
        logicTableExecutor.insert(
            "insert into target_table values(?, ?, ?)", "test1", "test2", "test3");
    System.out.println("pinsertCount = " + pinsertCount);

    final int unOrderInsertCount =
        logicTableExecutor.insert(
            "insert into target_table (eventCode, eventType, routeName) values(?, ?, ?)",
            "eventCode",
            "eventType",
            "routeName");
    System.out.println("unOrderInsertCount = " + unOrderInsertCount);

    final int pbatchCount =
        logicTableExecutor.insert(
            "insert into target_table values(?, ?, ?),(?, ?, ?)",
            "test1",
            "test2",
            "test3",
            "test4",
            "test5",
            "test6");
    System.out.println("pbatchCount = " + pbatchCount);

    final int pbatchCount2 =
        logicTableExecutor.insert(
            "insert into target_table values(?, ?, ?),('test4', 'test5', 'test5')",
            "test1",
            "test2",
            "test3");
    System.out.println("pbatchCount2 = " + pbatchCount2);
  }


  @Test
  public void testManulCodeTableConfig() {
    final FieldBasedTableResultResolver resultResolver =
        new FieldBasedTableResultResolver("code", 0, "data", "msg");

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
