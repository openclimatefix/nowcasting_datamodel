FROM python:3.10-slim

ARG TESTING=0

# make sure it doesnt fail if the docker file doesnt know the git commit
ARG GIT_PYTHON_REFRESH=quiet

# copy files
COPY setup.py app/setup.py
COPY README.md app/README.md
COPY requirements.txt app/requirements.txt
COPY .coveragerc app/.coveragerc

# install requirements
RUN pip install -r app/requirements.txt

# copy library files
COPY nowcasting_datamodel/ app/nowcasting_datamodel/
COPY tests/ app/tests/

# change to app folder
WORKDIR /app

# install library
RUN pip install -e .

RUN if [ "$TESTING" = 1 ]; then pip install pytest pytest-cov coverage; fi

# run migrations using this file
CMD ["python", "-u", "nowcasting_datamodel/migrations/app.py", "--run-migrations"]
