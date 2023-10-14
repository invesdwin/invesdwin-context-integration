package de.invesdwin.context.integration.channel.rpc.base.server.service;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Indicates that a specific service method is fast and should be executed directly in the IO thread instead of being
 * invoked by a worker thread. That way we can prevent a fast method call from being artificially slowed down because of
 * the context switched involved in worker threads. Though be sure to only use this for trivial things that involve no
 * IO, no cpu intensive calculations and no synchronization.
 * 
 * If this annotation is not present, the service call will be invoked in the worker executor. If it is present, worker
 * executor will be skipped. This is especially useful when the service method returns a future result and is quick to
 * return anyhow (should never block, instead throw exceptions). In that case the method can delegate into its own
 * executor or start some other async task without having to block a worker thread. This is also helpful to treat
 * specific requests with a higher priority than others.
 * 
 */
@Target({ ElementType.METHOD, ElementType.TYPE })
@Retention(RetentionPolicy.RUNTIME)
public @interface Blocking {

    /**
     * At the client side this annotation has no influence per default. When a method returns a future it is invoked
     * asynchronously, if it does not return a future it is invoked synchronously. Though if this flag is turned true,
     * then the client will synchronously wait for the future to be resolved, thus make a blocking call. That way only
     * the server is non-blocking while the client stays blocking.
     */
    boolean client() default false;

    /**
     * With this flag one could in make the client invoke a future blocking while keeping the server non-blocking
     * (client=true and server=false). Though this rarely makes sense so the default is to make the server blocking
     * here.
     */
    boolean server() default true;

}
