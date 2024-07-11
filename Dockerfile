FROM python:3.9-bullseye

ARG WORKDIR

# Specify workdir in image
WORKDIR /cumulative_advantage_brokerage

# Copy local files to workdir
ADD . /cumulative_advantage_brokerage

# Install packages
RUN \
    pip install --upgrade pip &&\
    pip install -r requirements.txt &&\
    pip install -e ./

# Print
CMD ["tail", "-f", "/dev/null"]