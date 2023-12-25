# This Dockerfile creates an image for running presubmit tests.
FROM openjdk:8
# Install Python and Basic Python Tools (Assuming VM has them)
RUN apt-get -y install python3 && apt-get update -y && apt clean
# Get pip to download and install requirements:
RUN apt-get -y install python3-pip && apt clean
COPY ./cloudbuild/python/cancel_job/requirements.txt /workspace/
RUN pip3 install -r /workspace/requirements.txt
