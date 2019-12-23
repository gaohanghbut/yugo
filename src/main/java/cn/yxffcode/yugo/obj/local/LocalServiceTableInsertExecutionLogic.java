package cn.yxffcode.yugo.obj.local;

import cn.yxffcode.yugo.obj.AbstractTableInsertExecutionLogic;
import org.apache.calcite.linq4j.tree.MethodCallExpression;

/** @author gaohang */
public class LocalServiceTableInsertExecutionLogic extends AbstractTableInsertExecutionLogic {
  public static LocalServiceTableInsertExecutionLogic getInstance() {
    return LocalServiceTableInsertExecutionLogicHolder.INSTANCE;
  }

  @Override
  protected MethodCallExpression callInsert(final String schemaName, final String tableName) {
    return null;
  }

  private static final class LocalServiceTableInsertExecutionLogicHolder {
    private static final LocalServiceTableInsertExecutionLogic INSTANCE =
        new LocalServiceTableInsertExecutionLogic();
  }
}
