package com.github.mauricio.async.db

/**
 *
 * Contains the SSL configuration necessary to connect to a database.
 *
 * @param enabled enable ssl, defaults to false
 * @param optional fallback to plaintext connection if server doesn't support ssl, defaults to false
 * @param rootFile path to PEM encoded trusted root certificates, None to use internal JDK cacerts, defaults to None
 * @param verifyRoot verify root certificate validity, defaults to true
 * @param verifyHostname verify peer certificate hostname, defaults to true
 *
 */
case class SSLConfiguration(enabled: Boolean = false,
                            optional: Boolean = false,
                            rootFile: Option[String] = None,
                            verifyRoot: Boolean = true,
                            verifyHostname: Boolean = true)

object SSLConfiguration {
  def apply(properties: Map[String, String]): SSLConfiguration = SSLConfiguration(
      enabled = properties.get("ssl").map(_.toBoolean).getOrElse(false),
      optional = properties.get("sslOptional").map(_.toBoolean).getOrElse(false),
      rootFile = properties.get("sslRootFile"),
      verifyRoot = properties.get("sslVerifyRoot").map(_.toBoolean).getOrElse(true),
      verifyHostname = properties.get("sslVerifyHostname").map(_.toBoolean).getOrElse(true))
}
