# Using roby944/jupyter as base image
FROM roby944/jupyter

# Set the working directory
WORKDIR /code

# Copy the current directory contents into the container
COPY . .

# Define command to run the app
CMD ["python", "prediction.py", "./output.json", "rho"]
