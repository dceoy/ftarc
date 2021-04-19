FROM ubuntu:20.04 AS builder

ENV DEBIAN_FRONTEND noninteractive

COPY --from=dceoy/samtools:latest /usr/local/src/samtools /usr/local/src/samtools
COPY --from=dceoy/bwa:latest /usr/local/src/bwa /usr/local/src/bwa
COPY --from=dceoy/bwa-mem2:latest /usr/local/src/bwa-mem2 /usr/local/src/bwa-mem2
COPY --from=dceoy/trim_galore:latest /usr/local/src/FastQC /usr/local/src/FastQC
COPY --from=dceoy/trim_galore:latest /usr/local/src/TrimGalore /usr/local/src/TrimGalore
COPY --from=dceoy/gatk:latest /opt/conda /opt/conda
COPY --from=dceoy/gatk:latest /opt/gatk /opt/gatk
ADD https://raw.githubusercontent.com/dceoy/print-github-tags/master/print-github-tags /usr/local/bin/print-github-tags
ADD . /tmp/ftarc

RUN set -e \
      && ln -sf bash /bin/sh

RUN set -e \
      && apt-get -y update \
      && apt-get -y dist-upgrade \
      && apt-get -y install --no-install-recommends --no-install-suggests \
        gcc libbz2-dev libcurl4-gnutls-dev liblzma-dev libncurses5-dev \
        libssl-dev libz-dev make pkg-config \
      && apt-get -y autoremove \
      && apt-get clean \
      && rm -rf /var/lib/apt/lists/*

ENV PATH /opt/gatk/bin:/opt/conda/envs/gatk/bin:/opt/conda/bin:${PATH}

RUN set -e \
      && source /opt/gatk/gatkenv.rc \
      && /opt/conda/bin/conda update -n base -c defaults conda \
      && /opt/conda/bin/python3 -m pip install -U --no-cache-dir \
        cutadapt /tmp/ftarc \
      && /opt/conda/bin/conda clean -yaf \
      && find /opt/conda -follow -type f -name '*.a' -delete \
      && find /opt/conda -follow -type f -name '*.pyc' -delete \
      && rm -rf /root/.cache/pip

RUN set -e \
      && cd /usr/local/src/bwa \
      && make clean \
      && make \
      && cd /usr/local/src/samtools/htslib-* \
      && make clean \
      && ./configure \
      && make \
      && make install \
      && cd /usr/local/src/samtools \
      && make clean \
      && ./configure \
      && make \
      && make install \
      && find \
        /usr/local/src/bwa /usr/local/src/bwa-mem2 /usr/local/src/FastQC \
        /usr/local/src/TrimGalore \
        -type f -executable -exec ln -s {} /usr/local/bin \;

FROM ubuntu:20.04

ENV DEBIAN_FRONTEND noninteractive

COPY --from=builder /usr/local /usr/local
COPY --from=builder /opt /opt

RUN set -e \
      && ln -sf bash /bin/sh \
      && echo '. /opt/conda/etc/profile.d/conda.sh' >> /etc/profile \
      && echo 'source activate gatk' >> /etc/profile \
      && echo 'source /opt/gatk/gatk-completion.sh' >> /etc/profile

RUN set -e \
      && apt-get -y update \
      && apt-get -y dist-upgrade \
      && apt-get -y install --no-install-recommends --no-install-suggests \
        apt-transport-https apt-utils ca-certificates curl gnupg gnuplot \
        libcurl3-gnutls libgsl23 libgkl-jni libncurses5 openjdk-8-jre pbzip2 \
        perl pigz python wget

RUN set -ea pipefail \
      && echo 'deb http://packages.cloud.google.com/apt cloud-sdk-bionic main' \
        | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list \
      && curl https://packages.cloud.google.com/apt/doc/apt-key.gpg \
        | apt-key add - \
      && apt-get -y update \
      && apt-get -y install --no-install-recommends --no-install-suggests \
        google-cloud-sdk \
      && apt-get -y autoremove \
      && apt-get clean \
      && rm -rf /var/lib/apt/lists/*

RUN set -e \
      && unlink /usr/lib/ssl/openssl.cnf \
      && echo -e 'openssl_conf = default_conf' > /usr/lib/ssl/openssl.cnf \
      && echo >> /usr/lib/ssl/openssl.cnf \
      && cat /etc/ssl/openssl.cnf >> /usr/lib/ssl/openssl.cnf \
      && echo >> /usr/lib/ssl/openssl.cnf \
      && echo -e '[default_conf]' >> /usr/lib/ssl/openssl.cnf \
      && echo -e 'ssl_conf = ssl_sect' >> /usr/lib/ssl/openssl.cnf \
      && echo >> /usr/lib/ssl/openssl.cnf \
      && echo -e '[ssl_sect]' >> /usr/lib/ssl/openssl.cnf \
      && echo -e 'system_default = system_default_sect' >> /usr/lib/ssl/openssl.cnf \
      && echo >> /usr/lib/ssl/openssl.cnf \
      && echo -e '[system_default_sect]' >> /usr/lib/ssl/openssl.cnf \
      && echo -e 'MinProtocol = TLSv1.2' >> /usr/lib/ssl/openssl.cnf \
      && echo -e 'CipherString = DEFAULT:@SECLEVEL=1' >> /usr/lib/ssl/openssl.cnf

ENV JAVA_LIBRARY_PATH /usr/lib/jni
ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64
ENV CLASSPATH /opt/gatk/gatk.jar:${CLASSPATH}
ENV PATH /opt/gatk/bin:/opt/conda/envs/gatk/bin:/opt/conda/bin:${PATH}

ENTRYPOINT ["/opt/conda/bin/ftarc"]
