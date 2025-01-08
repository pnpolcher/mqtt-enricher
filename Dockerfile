FROM python:3.12-slim-bookworm AS build

RUN pip install --no-cache-dir poetry
RUN mkdir /app
WORKDIR /app
COPY main.py /app
COPY pyproject.toml /app
COPY poetry.lock /app
RUN poetry config virtualenvs.create false \
    && poetry install --no-root

FROM python:3.12-slim-bookworm

RUN apt update && apt dist-upgrade -y
RUN apt-get clean && rm -rf -- /var/lib/apt/lists/*

WORKDIR /app
COPY --from=build /usr/local/lib/python3.12/site-packages /usr/local/lib/python3.12/site-packages
COPY . .
CMD ["python", "main.py"]
