package hadoop.test.job.mapper.isolated.scanned;

import javax.annotation.concurrent.Immutable;

import jakarta.inject.Named;

@Named
@Immutable
public class HadoopTestJobMapperIsolatedScannedBean {

    public boolean test() {
        return true;
    }

}
