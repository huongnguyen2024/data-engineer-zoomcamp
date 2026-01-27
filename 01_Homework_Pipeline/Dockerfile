# base Docker image that we will build on
FROM python:3.13.11-slim

# Copy uv binary from official uv image (multi-stage build pattern)
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/ 

# set up the working directory inside the container
WORKDIR /app

# Add virtual environment to PATH so we can use installed packages
ENV PATH="/app/.venv/bin:$PATH"

#these files were created by 'uv init' command
#Copy dependency files first (better caching)
COPY pyproject.toml .python-version uv.lock ./    
#Install all dependencies from lock file (ensures reproducible builds)    
RUN uv sync --locked                        

# copy the script to the container. 1st name is source file, 2nd is destination
COPY green_taxi_trips.py zones.py ./


# define what to do first when the container runs
# in this example, we will just run the script
ENTRYPOINT ["python"] 