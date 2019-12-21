package cn.yxffcode.yugo.obj.http;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.util.Sources;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.PrintStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/** @author gaohang */
public class HttpTableTest {

  @BeforeClass
  public static void start() throws ClassNotFoundException {
    Class.forName("org.apache.calcite.jdbc.Driver");
  }

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

  private List<Map<String, Object>> toRows(ResultSet rst) throws SQLException {
    final List<Map<String, Object>> rows = Lists.newArrayList();
    while (rst.next()) {
      final Map<String, Object> row = Maps.newHashMap();
      row.put("routeName", rst.getString("routeName"));
      row.put("eventType", rst.getString("eventType"));
      row.put("eventCode", rst.getString("eventCode"));
      rows.add(row);
    }
    return rows;
  }

  private void output(ResultSet resultSet, PrintStream out) throws SQLException {
    final ResultSetMetaData metaData = resultSet.getMetaData();
    final int columnCount = metaData.getColumnCount();
    while (resultSet.next()) {
      for (int i = 1; ; i++) {
        out.print(resultSet.getString(i));
        if (i < columnCount) {
          out.print(", ");
        } else {
          out.println();
          break;
        }
      }
    }
  }

  private void close(Connection connection) {
    if (connection != null) {
      try {
        connection.close();
      } catch (SQLException e) {
        // ignore
      }
    }
  }

  private String jsonPath(String model) {
    return resourcePath(model + ".json");
  }

  private String resourcePath(String path) {
    return Sources.of(HttpTableTest.class.getResource("/" + path)).file().getAbsolutePath();
  }
}
