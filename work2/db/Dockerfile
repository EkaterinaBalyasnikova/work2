FROM postgres:17

RUN apt-get update \
    && apt-get install -y locales \
    && echo "ru_RU.UTF-8 UTF-8" >> /etc/locale.gen \
    && locale-gen ru_RU.UTF-8 \
    && update-locale LANG=ru_RU.UTF-8 \
    && rm -rf /var/lib/apt/lists/*

ENV LANG ru_RU.UTF-8
ENV LC_ALL ru_RU.UTF-8


RUN echo "password_encryption = 'scram-sha-256'" >> /usr/share/postgresql/postgresql.conf.sample

COPY user.sql /docker-entrypoint-initdb.d/