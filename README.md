# yugo
yugo将接口映射成逻辑表，通过SQL调用接口，当没有复杂业务逻辑时，可用于多方系统对接.

## 使用示例
### 通过json配置表的schema
在maven工程的resources目录下创建httpapi.json文件，通过json配置两个表，分别是route_list和target_table，配置如下：
```json
{
  "version": "1.0",
  "defaultSchema": "httpapi",
  "schemas": [
    {
      "name": "httpapi",
      "type": "custom",
      "factory": "cn.yxffcode.yugo.obj.http.HttpApiSchemaFactory",
      "operand": {
        "checker": {//code=0时表示接口执行成功，取data作为结果，否则取msg作为错误信息，每个table的配置也可以有一个checker节点，如果没有，则以此为默认的
          "codeKey": "code",
          "successValue": 0,
          "dataKey": "data",
          "errMsgKey": "msg"
        },
        "tables": [
          {
            "name": "route_list",
            "url": "http://localhost:8088/route/list",
            "columns": [
              {
                "name": "page",
                "key": true //表示查询参数
              },
              {
                "name": "page_size",
                "key": true,
                "mapping": "limit"//表的字段名可以与接口中的数据路径不同，mapping支持对象图的访问，比如name在接口里可能是user.name
              },
              ["routeName", "eventType", "eventCode"] //只需要字段名的情况
            ]
          },
          {
            "name": "target_table",
            "url": "http://localhost:8088/test/insert",
            "method": "post",
            "columns": [
              ["routeName", "eventType", "eventCode"]
            ]
          }
        ]
      }
    }
  ]
}
```

执行查询与更新：

```java
//以httpapi.json配置创建LogicTableExecutor
final LogicTableExecutor logicTableExecutor = new LogicTableExecutor("httpapi");

//查询route_list
final List<Map<String, Object>> values =
    logicTableExecutor.select("select * from route_list where page = 1 and page_size = 100");
System.out.println("values = " + values);

//插入target_table表, insertCount = 1
final int insertCount = logicTableExecutor.insert("insert into target_table values('test1', 'test2', 'test3')");
System.out.println("insertCount = " + insertCount);

//批量插入2条数据到target_table表， batchCount = 2
final int batchCount =
    logicTableExecutor.insert(
        "insert into target_table values('test1', 'test2', 'test3'),('test4', 'test5', 'test5')");
System.out.println("batchCount = " + batchCount);

//带参数的sql，查询
final List<Map<String, Object>> pvalues =
    logicTableExecutor.select(
        "select * from route_list where page = ? and page_size = ?", 1, 100);
System.out.println("pvalues = " + pvalues);

//带参数的sql，插入到target_table表，pinsertCount=1
final int pinsertCount =
    logicTableExecutor.insert(
        "insert into target_table values(?, ?, ?)", "test1", "test2", "test3");
System.out.println("pinsertCount = " + pinsertCount);

//带参数的sql，批量插入2条数据到target_table表，pbatchCount=2
final int pbatchCount =
    logicTableExecutor.insert(
        "insert into target_table values(?, ?, ?),(?, ?, ?)", "test1", "test2", "test3", "test4", "test5", "test5");
System.out.println("pbatchCount = " + pbatchCount);

//指定字段插入，unOrderInsertCount = 1
final int unOrderInsertCount =
    logicTableExecutor.insert(
        "insert into target_table (eventCode, eventType, routeName) values(?, ?, ?)", "eventCode", "eventType", "routeName");
System.out.println("unOrderInsertCount = " + unOrderInsertCount);

```
### 通过代码注册逻辑表
```java
//创建TableResultResolver，与json配置中的checker节点相同
final HttpTableResultResolver resultResolver =
    new HttpTableResultResolver("code", 0, "data", "msg");

//创建逻辑表配置，将http接口映射为逻辑表，类似于json配置中的tables节点
SchemaDefBuilder.http()
    .table("http://localhost:8088/route/list")
    .name("route_list")
    .columns("routeName", "eventType", "eventCode", "page")
    .column("pageSize", "limit")//name=pageSize, mapping=limit
    .tableResultResolver(resultResolver)
    .<SchemaDefBuilder.HttpSchemaDefBuilder>register()
    .table("http://localhost:8088/test/insert")
    .name("target_table")
    .columns("routeName", "eventType", "eventCode")
    .tableResultResolver(resultResolver)
    .register()
    .build();
//创建LogicTableExecutor
final LogicTableExecutor logicTableExecutor = new LogicTableExecutor();
//逻辑表的使用与json配置的一样
final List<Map<String, Object>> values =
    logicTableExecutor.select("select * from route_list where page = 1 and pageSize = 100");
System.out.println("values = " + values);
```

###将逻辑表的配置存储在数据库中
有了上面两种逻辑表的配置方式，如果将表的配置存储在数据库中，系统启动时加载并创建逻辑表将变得非常容易

### 使用jdbc接口
calcite的支持，详细见calcite的文档：
```java

@Test
public void testHttpApi() throws SQLException, ClassNotFoundException {
Connection connection = null;
try {
  Properties info = new Properties();
  info.put("model", jsonPath("httpapi"));
  info.setProperty("caseSensitive", "false");
  connection = DriverManager.getConnection("jdbc:calcite:", info);
  CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);
  prepare(calciteConnection);
  prepare(calciteConnection);
  prepare(calciteConnection);
} finally {
  close(connection);
}
}

private void prepare(final CalciteConnection calciteConnection) throws SQLException {
final PreparedStatement cstate =
    calciteConnection.prepareStatement(
        "insert into target_table (select routeName, eventType, eventCode from route_list where page = 1 and page_size = 100 and routeName > 'c')");

System.out.println("rs = " + cstate.executeUpdate());
System.out.println("rs = " + cstate.executeUpdate());
System.out.println("rs = " + cstate.executeUpdate());
System.out.println("rs = " + cstate.executeUpdate());
}

```
## UDF
当需要一点转换逻辑时，可使用UDF，比如对日期做格式化等，目前还没支持。
