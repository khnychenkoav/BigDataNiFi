FROM apache/nifi:1.28.1

ADD https://jdbc.postgresql.org/download/postgresql-42.7.4.jar /opt/nifi/nifi-current/lib/postgresql-42.7.4.jar

USER root
RUN chown nifi:nifi /opt/nifi/nifi-current/lib/postgresql-42.7.4.jar
USER nifi
