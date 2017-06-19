FROM java:8

# Install Java.
RUN apt-get update -y && apt-get upgrade -y

# Define commonly used JAVA_HOME variable
ENV JAVA_HOME /usr/lib/jvm/java-8-oracle

EXPOSE 8081:8081
ADD /target/lobby-4.0.jar app.jar
ENTRYPOINT ["java", "-jar", "app.jar"]