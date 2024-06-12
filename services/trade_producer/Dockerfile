FROM python:3.12.3-slim

#stream output to console
ENV PYTHONUNBUFFERED=1

RUN pip install poetry==1.8.3

WORKDIR /app

#copy the pyproject.toml and poetry.lock files into the container
COPY pyproject.toml poetry.lock /app/

#install Python dependencies from the pyproject.toml file
RUN poetry install

#copy all the source code into the container
COPY . /app


CMD ["poetry", "run", "python", "src/main.py"]
