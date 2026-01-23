# base Docker image that we will build on
FROM python:3.13.11-slim

# set up our image by installing prerequistites; pandas in this case
#RUN pip install pandas pyarrow
# Copy uv binary from official uv image (multi-stage build pattern)
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/

# set up the working directory inside the container
WORKDIR /code

# copy the script to the container. 1st name is source file, 2nd is the destination
COPY pipeline.py .
# copy dependencies files first 
COPY ".python-version" "pyproject.toml" "uv.lock" ./

# Install dependencies from "uv.lock" file to ensure the reproducible builds (same dependencies as the environment build)
RUN uv sync --locked

# define what to do first when the container runs
# in this example we will just run the script
# adding ENTRYPOINT so that we dont have to type the same entry everytime we run the container
# ENTRYPOINT ["uv", "run", "python", "pipeline.py"]
# instead of "uv", "run" for ENTRYPOINT, use PATH to venv 
# by adding virtual environment to PATH, we point to venv without writing command to activate the venv everytime
ENV PATH="/code/.venv/bin:$PATH"
ENTRYPOINT ["python", "pipeline.py"]
