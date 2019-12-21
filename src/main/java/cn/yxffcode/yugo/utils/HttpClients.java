package cn.yxffcode.yugo.utils;

import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;

/** @author gaohang */
public final class HttpClients {
  public static final CloseableHttpClient HTTP_CLIENT = HttpClientBuilder.create().build();

  private HttpClients() {}
}
