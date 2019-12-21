# yugo
yugo将接口映射成逻辑表，通过SQL调用接口，当没有复杂业务逻辑时，可用于多方系统对接.

## 使用示例
### 通过json配置表的schema
先通过json配置两个表，分别是route_list和target_table，配置如下：
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
        "checker": {//code=0时表示接口执行成功，取data作为结果，否则取msg作为错误信息
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
                "mapping": "limit"
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
//创建LogicTableExecutor
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
## UDF
当需要一点转换逻辑时，可使用UDF，比如对日期做格式化等，目前还没支持。
