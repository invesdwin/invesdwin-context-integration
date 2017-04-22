package de.invesdwin.context.integration.ws.jaxrs;

import javax.annotation.concurrent.NotThreadSafe;
import javax.xml.bind.annotation.XmlRootElement;

import de.invesdwin.util.bean.AValueObject;

@XmlRootElement
@NotThreadSafe
public class SampleValueObject extends AValueObject {

    private String getIt;

    public String getGetIt() {
        return getIt;
    }

    public void setGetIt(final String getIt) {
        this.getIt = getIt;
    }

}
