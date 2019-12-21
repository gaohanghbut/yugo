package cn.yxffcode.yugo.def;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.calcite.util.Closer;
import org.apache.calcite.util.Sources;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * TODO:使用连接池管理连接，减少开销
 *
 * @author gaohang
 */
public class LogicTableExecutor {
  private static final String CALCITE_URL = "jdbc:calcite:";

  static {
    try {
      Class.forName("org.apache.calcite.jdbc.Driver");
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  private final Properties info;

  public LogicTableExecutor() {
    this("configurable.schema");
  }

  public LogicTableExecutor(final String configPath) {
    Properties info = new Properties();
    info.put("model", jsonPath(configPath));
    info.setProperty("caseSensitive", "false");
    this.info = info;
  }

  public int insert(final String sql, Object... params) {
    try (final Connection connection = DriverManager.getConnection(CALCITE_URL, info);
        final PreparedStatement statement = connection.prepareStatement(sql)) {
      for (int i = 0; i < params.length; i++) {
        statement.setObject(i + 1, params[i]);
      }
      return statement.executeUpdate();
    } catch (SQLException e) {
      throw new SQLExecutorException(e);
    }
  }

  public List<Map<String, Object>> select(final String sql, Object... params) {
    try (final Closer closer = new Closer()) {
      final Connection connection = closer.add(DriverManager.getConnection(CALCITE_URL, info));
      final PreparedStatement statement = connection.prepareStatement(sql);
      for (int i = 0; i < params.length; i++) {
        statement.setObject(i + 1, params[i]);
      }
      final ResultSet resultSet = statement.executeQuery();
      final ResultSetMetaData metaData = resultSet.getMetaData();
      final int columnCount = metaData.getColumnCount();

      final List<Map<String, Object>> rsts = Lists.newArrayList();
      while (resultSet.next()) {
        final Map<String, Object> results = Maps.newHashMapWithExpectedSize(columnCount);
        for (int i = 1; i <= columnCount; i++) {
          final String columnName = metaData.getColumnName(i);
          results.put(columnName, resultSet.getObject(i));
        }
        rsts.add(results);
      }
      return rsts;
    } catch (SQLException e) {
      throw new SQLExecutorException(e);
    }
  }

  private String jsonPath(String model) {
    return resourcePath(model + ".json");
  }

  private String resourcePath(String path) {
    return Sources.of(LogicTableExecutor.class.getResource("/" + path)).file().getAbsolutePath();
  }
}
