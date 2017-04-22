package de.invesdwin.context.integration.ws.registry.publication;

import java.io.IOException;
import java.net.URI;

import javax.annotation.concurrent.ThreadSafe;
import javax.xml.parsers.ParserConfigurationException;

import org.springframework.beans.factory.BeanNameAware;
import org.springframework.core.io.ClassPathResource;
import org.springframework.ws.wsdl.wsdl11.DefaultWsdl11Definition;
import org.springframework.xml.xsd.SimpleXsdSchema;
import org.xml.sax.SAXException;

import de.invesdwin.context.integration.IntegrationProperties;
import de.invesdwin.context.log.error.Err;
import de.invesdwin.util.lang.uri.URIs;

/**
 * Publishes a wsdl by the bean id. Expects an xsd in META-INF/xsd with the same name as the bean id.
 * 
 * The wsdl will get published at: http://host:port/spring-ws/beanid.wsdl
 * 
 * @author subes
 * 
 */
@ThreadSafe
public class XsdWebServicePublication extends DefaultWsdl11Definition implements IWebServicePublication, BeanNameAware {

    private static final String LOCATION = "/spring-ws/";

    private volatile String name;
    private volatile boolean useRegistry = true;

    @Override
    public void setUseRegistry(final boolean useRegistry) {
        this.useRegistry = useRegistry;
    }

    @Override
    public boolean isUseRegistry() {
        return useRegistry;
    }

    @Override
    public void setBeanName(final String n) {
        this.name = n;
        //Configuration matches the following xml code::
        //<bean id="<name>" class="org.springframework.ws.wsdl.wsdl11.DefaultWsdl11Definition">
        //  <property name="schema">
        //      <bean id="schema" class="org.springframework.xml.xsd.SimpleXsdSchema">
        //          <property name="xsd" value="classpath:/META-INF/<name>.xsd" />
        //      </bean>
        //  </property>
        //  <property name="portTypeName" value="<name>" />
        //  <property name="locationUri" value="/spring-ws/" />
        //</bean>
        try {
            final ClassPathResource xsd = new ClassPathResource("/META-INF/xsd/" + name + ".xsd");
            final SimpleXsdSchema schema = new SimpleXsdSchema();
            schema.setXsd(xsd);
            schema.afterPropertiesSet();
            super.setSchema(schema);
            super.setPortTypeName(name);
            super.setLocationUri(LOCATION);
        } catch (final ParserConfigurationException e) {
            throw Err.process(e);
        } catch (final IOException e) {
            throw Err.process(e);
        } catch (final SAXException e) {
            throw Err.process(e);
        }
    }

    @Override
    public URI getUri() {
        return newUri(IntegrationProperties.WEBSERVER_BIND_URI, name);
    }

    public static URI newUri(final URI baseUri, final String serviceName) {
        return URIs.asUri(baseUri + LOCATION + serviceName + ".wsdl");
    }

    @Override
    public String getServiceName() {
        return name;
    }

}
