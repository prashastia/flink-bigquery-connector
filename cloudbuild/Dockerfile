# This Dockerfile creates an image for running presubmit tests.
FROM openjdk:8

RUN \
  echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] http://packages.cloud.google.com/apt cloud-sdk main" | \
  tee -a /etc/apt/sources.list.d/google-cloud-sdk.list && \
  curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | \
  apt-key --keyring /usr/share/keyrings/cloud.google.gpg  add - && \
  apt-get update -y && \
  apt-get install google-cloud-cli -y

# Add the current directory to the container
COPY ./cloudbuild/python_scripts /workspace/python_scripts/

# Install Python and Basic Python Tools (Assuming VM has them)
RUN apt-get install -y python python-dev python-setuptools
RUN apt-get install -y python-pip 

# Get pip to download and install requirements:
RUN pip install -r /workspace/python_scripts/requirements.txt
