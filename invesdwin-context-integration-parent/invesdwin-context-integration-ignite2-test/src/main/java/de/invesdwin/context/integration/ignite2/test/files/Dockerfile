FROM eclipse-temurin:17-jdk

ENV IGNITE_HOME=/opt/ignite

# Copy extracted Ignite distribution from the local working folder
COPY ignite /opt/ignite

# Add JVM options required for Java 17 compatibility with Apache Ignite
ENV JVM_OPTS="--add-opens=java.base/jdk.internal.misc=ALL-UNNAMED \
    --add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
    --add-opens=java.base/java.nio=ALL-UNNAMED \
    --add-opens=java.base/java.util=ALL-UNNAMED \
    --add-opens=java.base/java.lang=ALL-UNNAMED \
    --add-opens=java.base/java.lang.invoke=ALL-UNNAMED \
    --add-opens=java.base/java.util.concurrent=ALL-UNNAMED \
    --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED \
    --add-opens=java.base/java.lang.reflect=ALL-UNNAMED \
    --add-opens=java.base/java.text=ALL-UNNAMED \
    --add-opens=java.sql/java.sql=ALL-UNNAMED"

EXPOSE 47500 47100

CMD ["/opt/ignite/bin/ignite.sh", "/opt/ignite/config/custom-config.xml"]