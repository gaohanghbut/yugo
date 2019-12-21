package org.apache.calcite.interpreter;

import org.apache.calcite.DataContext;

/** @author gaohang */
public final class ContextFactory {
  private ContextFactory() {}

  public static Context createContext(final DataContext dataContext) {
    return new Context(dataContext);
  }
}
