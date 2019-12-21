package cn.yxffcode.yugo.obj;

/**
 * 不支持的sql语句异常
 *
 * @author gaohang
 */
public final class UnsupportedStatementException extends SQLStatementException {
  public UnsupportedStatementException(final String message) {
    super(message);
  }
}
