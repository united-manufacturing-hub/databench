# Use the official rust image as the base image
FROM rust:slim AS build

# Install cmake and compile dependencies
RUN apt-get update && apt-get install -y cmake gcc g++ pkg-config libssl-dev git

# Set the working directory
WORKDIR /app

# Copy the Cargo.toml and src files
COPY Cargo.toml .
COPY src src
COPY src/generator/chernobyl/powerplant.json .

# Compile the program (statically)
RUN cargo build --release

# Use a lightweight base image for the final image
FROM debian:stable-slim

# Set the working directory
WORKDIR /app

# Copy the compiled binary and powerplant.json from the build stage
COPY --from=build /app/target/release/databench-rs generator

# Set executable permissions for the binary
RUN chmod +x generator

# Set the binary as the default command to run
CMD ["./generator"]
