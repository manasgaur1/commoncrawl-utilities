# Use an official Python runtime as a parent image
FROM python:3.10.9

# Set the working directory to /app
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY commonCrawl.py /app
COPY requirements.txt /app
COPY config/config.json /app/config/config.json

# Install any needed packages specified in requirements.txt
RUN pip install --trusted-host pypi.python.org -r requirements.txt

# Make port 80 available to the world outside this container
EXPOSE 80

# Define environment variable for the URL parameter
ENV URL_PARAM=

# Run app.py when the container launches with the provided URL parameter
CMD ["python", "commonCrawl.py"]
