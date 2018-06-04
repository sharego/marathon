package mesosphere.marathon

import org.aopalliance.intercept.MethodInvocation

package object metrics {
  def className(klass: Class[_]): String = {
    if (klass.getName.contains("$EnhancerByGuice$")) klass.getSuperclass.getName else klass.getName
  }

  def name(originalName: String): String = {
    originalName.replace('$', '.').replaceAll("""\.+""", ".")
  }

  def name(prefix: MetricPrefix, klass: Class[_], originalName: String): String = {
    name(s"${prefix.name}.${className(klass)}.$originalName")
  }

  def name(prefix: MetricPrefix, in: MethodInvocation): String = {
    name(prefix, in.getThis.getClass, in.getMethod.getName)
  }
}
