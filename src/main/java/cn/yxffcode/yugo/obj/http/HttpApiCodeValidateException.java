package cn.yxffcode.yugo.obj.http;

import cn.yxffcode.yugo.obj.SQLStatementException;

/**
 * 表示http接口的返回数据中的code校验错误
 *
 * @author gaohang
 */
public class HttpApiCodeValidateException extends SQLStatementException {
  public HttpApiCodeValidateException(final String message) {
    super(message);
  }
}
