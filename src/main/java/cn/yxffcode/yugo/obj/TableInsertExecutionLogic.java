package cn.yxffcode.yugo.obj;

import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.enumerable.EnumerableRelImplementor;
import org.apache.calcite.adapter.java.JavaTypeFactory;

import java.util.List;

/**
 * @author gaohang
 */
public interface TableInsertExecutionLogic {
  List<ParameterIndex> getParameterIndex(
      ObjectTableModify tableModify, JavaTypeFactory typeFactory);

  EnumerableRel.Result implement(
      ObjectTableModify tableModify,
      List<ParameterIndex> parameterIndex,
      EnumerableRelImplementor implementor,
      EnumerableRel.Prefer pref);
}
