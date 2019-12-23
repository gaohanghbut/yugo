package cn.yxffcode.yugo.def;

/** @author gaohang */
enum SchemaType {
  HTTP("http"),

  LOCAL("local");

  final String name;

  SchemaType(final String name) {
    this.name = name;
  }
}
