# Use the official Go image as the base image
FROM golang AS build

# Set the working directory
WORKDIR /app

# Copy the Go modules and download them
COPY go.mod go.sum ./
RUN go mod download

# Copy the rest of the code
COPY . .

# Compile the program
RUN go build -o generator .

# Use a lightweight base image for the final image
FROM alpine

# Set the working directory
WORKDIR /app

# Copy the compiled binary and powerplant.json from the build stage
COPY --from=build /app/generator .
COPY --from=build /app/run.sh .
COPY --from=build /app/powerplant/powerplant.json powerplant/

# Set executable permissions for the binary
RUN chmod +x generator
RUN chmod +x run.sh

# Set the binary as the default command to run
CMD ["./run.sh"]
