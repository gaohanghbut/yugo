package cn.yxffcode.yugo.obj;

import net.sf.cglib.beans.BeanMap;

import java.util.Map;
import java.util.Objects;

/**
 * 校验http接口
 *
 * @author gaohang
 */
public final class FieldBasedTableResultResolver implements TableResultResolver {
  private static final String DEFAULT_CODE_KEY = "code";
  private static final int DEFAULT_SUCCESS_VALUE = 200;
  private static final String DEFAULT_DATA_KEY = "data";
  private static final String DEFAULT_ERR_MSG_KEY = "errMsg";
  private String codeKey;
  private Object successValue;
  private String dataKey;
  private String errMsgKey;

  public FieldBasedTableResultResolver(
      final String codeKey,
      final Object successValue,
      final String dataKey,
      final String errMsgKey) {
    this.codeKey = codeKey;
    this.successValue = successValue;
    this.dataKey = dataKey;
    this.errMsgKey = errMsgKey;
    nullToDefault();
  }

  public FieldBasedTableResultResolver(final Map<String, Object> checker) {
    if (checker != null) {
      this.codeKey = (String) checker.get("codeKey");
      this.successValue = checker.get("successValue");
      this.dataKey = (String) checker.get("dataKey");
      this.errMsgKey = (String) checker.get("errMsgKey");
    }

    nullToDefault();
  }

  private void nullToDefault() {
    if (this.codeKey == null) {
      this.codeKey = DEFAULT_CODE_KEY;
    }
    if (successValue == null) {
      this.successValue = DEFAULT_SUCCESS_VALUE;
    }
    if (dataKey == null) {
      this.dataKey = DEFAULT_DATA_KEY;
    }
    if (errMsgKey == null) {
      this.errMsgKey = DEFAULT_ERR_MSG_KEY;
    }
  }

  @Override
  public void validate(final Object result) {
    final Map map = asMap(result);
    if (!map.containsKey(codeKey)) {
      return;
    }
    final Object code = map.get(codeKey);
    if (!Objects.equals(code, successValue)) {
      throw new ApiCodeValidateException(
          "code validate failed, successCode is " + successValue + " actual code is " + code);
    }
  }

  public void validate0(final Map data) {
    if (!data.containsKey(codeKey)) {
      return;
    }
    final Object code = data.get(codeKey);
    if (!Objects.equals(code, successValue)) {
      throw new ApiCodeValidateException(
          "code validate failed, successCode is " + successValue + " actual code is " + code);
    }
  }

  @Override
  public Object data(final Object result) {
    final Map map = asMap(result);
    validate0(map);
    return map.get(dataKey);
  }

  private Map asMap(final Object result) {
    if (result instanceof Map) {
      return (Map) result;
    }
    return BeanMap.create(result);
  }
}
