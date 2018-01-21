package de.invesdwin.context.integration.jppf.server.test;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * With this annotation you can mark tests so that they enable the ftp server during test execution.
 * 
 * You can also disable the ftp server by putting "false" as the value to override an enabled ftp server from a base
 * class.
 */
@Target({ ElementType.TYPE })
@Retention(RetentionPolicy.RUNTIME)
public @interface JPPFServerTest {

    boolean value() default true;

}
