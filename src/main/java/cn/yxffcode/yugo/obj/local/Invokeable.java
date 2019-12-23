package cn.yxffcode.yugo.obj.local;

import java.lang.reflect.InvocationTargetException;

/** @author gaohang */
public interface Invokeable {
  Object invoke(Object... args) throws InvocationTargetException;
}
