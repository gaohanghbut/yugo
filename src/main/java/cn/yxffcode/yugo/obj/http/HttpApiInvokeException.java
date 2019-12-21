package cn.yxffcode.yugo.obj.http;

import cn.yxffcode.yugo.obj.SQLStatementException;

/**
 * http调用异常
 *
 * @author gaohang
 */
final class HttpApiInvokeException extends SQLStatementException {
  HttpApiInvokeException(final String message, final Throwable cause) {
    super(message, cause);
  }
}
