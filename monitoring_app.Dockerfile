FROM python:3.12-alpine

COPY . /app/sc_wgs_monitoring

RUN \
    apk add --no-cache gcc musl-dev linux-headers && \
    pip install --quiet --upgrade pip && \
    pip install -r /app/sc_wgs_monitoring/requirements.txt && \

    echo "Deleting cache files and removing build dependencies" 1>&2 && \
    find /usr/local/lib/python3.12  \( -iname '*.c' -o -iname '*.pxd' -o -iname '*.pyd' -o -iname '__pycache__' \) | \
    xargs rm -rf {} && \
    rm -rf /root/.cache/pip && \
    apk --purge del gcc musl-dev linux-headers