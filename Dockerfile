# Use a lightweight Python 3.9 image
FROM python:3.9-slim

# Prevent Python from creating .pyc files and buffer output
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Set working directory
WORKDIR /app

# Copy the requirements file and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the application code
COPY extract.py .
COPY preprocessing.py .
COPY main.py .

# Command to run the application
CMD ["python", "main.py"]