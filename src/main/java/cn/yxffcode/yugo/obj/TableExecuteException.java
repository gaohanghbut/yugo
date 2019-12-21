package cn.yxffcode.yugo.obj;

public final class TableExecuteException extends SQLStatementException {
  public TableExecuteException(final Throwable cause) {
    super(cause);
  }

  public TableExecuteException(final String message, final Throwable cause) {
    super(message, cause);
  }
}
