package cn.yxffcode.yugo.obj;

/**
 * SQL处理相关异常的父类
 *
 * @author gaohang
 */
public abstract class SQLStatementException extends RuntimeException {
  public SQLStatementException(final String message) {
    super(message);
  }

  public SQLStatementException(final Throwable cause) {
    super(cause);
  }

  public SQLStatementException(final String message, final Throwable cause) {
    super(message, cause);
  }
}
