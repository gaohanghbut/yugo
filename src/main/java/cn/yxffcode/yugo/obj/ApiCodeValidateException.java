package cn.yxffcode.yugo.obj;

/**
 * 表示http接口的返回数据中的code校验错误
 *
 * @author gaohang
 */
final class ApiCodeValidateException extends SQLStatementException {
  ApiCodeValidateException(final String message) {
    super(message);
  }
}
