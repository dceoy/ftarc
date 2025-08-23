# syntax=docker/dockerfile:1
ARG UBUNTU_VERSION=24.04
ARG PYTHON_VERSION=3.13

FROM public.ecr.aws/docker/library/ubuntu:${UBUNTU_VERSION} AS builder

ARG PYTHON_VERSION

ENV DEBIAN_FRONTEND=noninteractive
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PYTHONIOENCODING=utf-8
ENV PIP_DISABLE_PIP_VERSION_CHECK=1
ENV PIP_NO_CACHE_DIR=1
ENV UV_COMPILE_BYTECODE=1
ENV UV_LINK_MODE=copy

SHELL ["/bin/bash", "-euo", "pipefail", "-c"]

# Copy tool sources from other images
COPY --from=dceoy/samtools:latest /usr/local/src/samtools /usr/local/src/samtools
COPY --from=dceoy/bwa:latest /usr/local/src/bwa /usr/local/src/bwa
COPY --from=dceoy/bwa-mem2:latest /usr/local/src/bwa-mem2 /usr/local/src/bwa-mem2
COPY --from=dceoy/trim_galore:latest /usr/local/src/FastQC /usr/local/src/FastQC
COPY --from=dceoy/trim_galore:latest /usr/local/src/TrimGalore /usr/local/src/TrimGalore
COPY --from=dceoy/gatk:latest /opt/conda /opt/conda
COPY --from=dceoy/gatk:latest /opt/gatk /opt/gatk

# Setup apt cache for BuildKit
RUN \
      rm -f /etc/apt/apt.conf.d/docker-clean \
      && echo 'Binary::apt::APT::Keep-Downloaded-Packages "true";' \
        > /etc/apt/apt.conf.d/keep-cache

# Install build dependencies
# hadolint ignore=DL3008
RUN \
      --mount=type=cache,target=/var/cache/apt,sharing=locked \
      --mount=type=cache,target=/var/lib/apt,sharing=locked \
      apt-get -y update \
      && apt-get -y upgrade \
      && apt-get -y install --no-install-recommends --no-install-suggests \
        gcc libbz2-dev libcurl4-gnutls-dev liblzma-dev libncurses5-dev \
        libssl-dev libz-dev make pkg-config curl ca-certificates

# Setup Python and pip
ENV PATH=/opt/gatk/bin:/opt/conda/envs/gatk/bin:/opt/conda/bin:${PATH}

RUN \
      --mount=type=cache,target=/root/.cache \
      source /opt/gatk/gatkenv.rc \
      && /opt/conda/bin/conda update -n base -c defaults conda \
      && curl -SL -o /tmp/get-pip.py https://bootstrap.pypa.io/get-pip.py \
      && /opt/conda/bin/python3 /tmp/get-pip.py \
      && /opt/conda/bin/python3 -m pip install --upgrade pip uv \
      && rm -f /tmp/get-pip.py

# Install Python packages including ftarc
RUN \
      --mount=type=cache,target=/root/.cache \
      --mount=type=bind,source=.,target=/mnt/host \
      cp -a /mnt/host /tmp/ftarc \
      && /opt/conda/bin/python3 -m pip install --no-cache-dir \
        cutadapt /tmp/ftarc \
      && /opt/conda/bin/conda clean -yaf \
      && find /opt/conda -follow -type f -name '*.a' -delete \
      && find /opt/conda -follow -type f -name '*.pyc' -delete

# Build and install genomics tools
RUN \
      cd /usr/local/src/bwa \
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


FROM public.ecr.aws/docker/library/ubuntu:${UBUNTU_VERSION} AS runtime

ARG PYTHON_VERSION
ARG USER_NAME=ftarc
ARG USER_UID=1001
ARG USER_GID=1001

COPY --from=builder /usr/local /usr/local
COPY --from=builder /opt /opt
COPY --from=builder /etc/apt/apt.conf.d/keep-cache /etc/apt/apt.conf.d/keep-cache

ENV DEBIAN_FRONTEND=noninteractive
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PYTHONIOENCODING=utf-8

SHELL ["/bin/bash", "-euo", "pipefail", "-c"]

# Setup profile
RUN \
      echo '. /opt/conda/etc/profile.d/conda.sh' >> /etc/profile \
      && echo 'source activate gatk' >> /etc/profile \
      && echo 'source /opt/gatk/gatk-completion.sh' >> /etc/profile \
      && rm -f /etc/apt/apt.conf.d/docker-clean

# Install runtime dependencies
# hadolint ignore=DL3008
RUN \
      --mount=type=cache,target=/var/cache/apt,sharing=locked \
      --mount=type=cache,target=/var/lib/apt,sharing=locked \
      apt-get -y update \
      && apt-get -y upgrade \
      && apt-get -y install --no-install-recommends --no-install-suggests \
        apt-transport-https apt-utils ca-certificates curl gnupg gnuplot \
        libcurl3-gnutls libgsl23 libgkl-jni libncurses5 openjdk-8-jre pbzip2 \
        perl pigz python wget

# Install Google Cloud SDK
# hadolint ignore=DL3008,DL3009
RUN \
      --mount=type=cache,target=/var/cache/apt,sharing=locked \
      --mount=type=cache,target=/var/lib/apt,sharing=locked \
      echo 'deb http://packages.cloud.google.com/apt cloud-sdk-bionic main' \
        | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list \
      && curl https://packages.cloud.google.com/apt/doc/apt-key.gpg \
        | apt-key add - \
      && apt-get -y update \
      && apt-get -y install --no-install-recommends --no-install-suggests \
        google-cloud-sdk

# Configure OpenSSL for legacy support
RUN \
      unlink /usr/lib/ssl/openssl.cnf \
      && { \
        echo 'openssl_conf = default_conf'; \
        echo; \
        cat /etc/ssl/openssl.cnf; \
        echo; \
        echo '[default_conf]'; \
        echo 'ssl_conf = ssl_sect'; \
        echo; \
        echo '[ssl_sect]'; \
        echo 'system_default = system_default_sect'; \
        echo; \
        echo '[system_default_sect]'; \
        echo 'MinProtocol = TLSv1.2'; \
        echo 'CipherString = DEFAULT:@SECLEVEL=1'; \
      } > /usr/lib/ssl/openssl.cnf

# Set Java environment
ENV JAVA_LIBRARY_PATH=/usr/lib/jni
ENV JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
ENV CLASSPATH=/opt/gatk/gatk.jar:${CLASSPATH}
ENV PATH=/opt/gatk/bin:/opt/conda/envs/gatk/bin:/opt/conda/bin:${PATH}

# Create non-root user
RUN \
      groupadd --gid "${USER_GID}" "${USER_NAME}" \
      && useradd --uid "${USER_UID}" --gid "${USER_GID}" --shell /bin/bash --create-home "${USER_NAME}"

USER "${USER_NAME}"

HEALTHCHECK NONE

ENTRYPOINT ["/opt/conda/bin/ftarc"]