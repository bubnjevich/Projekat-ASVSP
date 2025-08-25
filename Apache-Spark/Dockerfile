FROM bde2020/spark-master:3.0.1-hadoop3.2

RUN apk update && apk add --no-cache \
    openssh-server python3 py3-pip curl \
    python3-dev gfortran gcc g++ musl-dev openblas-dev \
    && mkdir /var/run/sshd \
    && ssh-keygen -A \
    && sed -i 's/#PermitRootLogin prohibit-password/PermitRootLogin yes/' /etc/ssh/sshd_config \
    && sed -i 's/#PasswordAuthentication yes/PasswordAuthentication yes/' /etc/ssh/sshd_config \
    && echo 'export JAVA_HOME=/usr/lib/jvm/java-1.8-openjdk/' >> /spark/bin/load-spark-env.sh \
    && echo "root:asvsp" | chpasswd


RUN python3 -m pip install --upgrade pip setuptools wheel \
 && python3 -m pip install "numpy<2.0" pyspark==3.0.1

RUN python3 --version && pip3 show pyspark

# PostgreSQL driver
RUN curl -fSL -o /spark/jars/postgresql-42.2.29.jar \
    https://repo1.maven.org/maven2/org/postgresql/postgresql/42.2.29/postgresql-42.2.29.jar

EXPOSE 22
