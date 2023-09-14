# Use the official rust image as the base image
FROM rust:alpine AS build

# Add gnu toolchain
RUN apk add --no-cache gcc musl-dev
# Add pthreads, zlib and openssl, cmake, make
RUN apk add --no-cache libpthread-stubs musl-dev zlib-dev openssl-dev cmake make
# Add g++ and libstdc++
RUN apk add --no-cache g++ libstdc++

# Set the working directory
WORKDIR /app

# Copy the Cargo.toml and src files
COPY Cargo.toml .
COPY src src
COPY powerplant.json .

# Compile the program (statically)
RUN cargo build --release

# Use a lightweight base image for the final image
FROM alpine

# Set the working directory
WORKDIR /app

# Copy the compiled binary and powerplant.json from the build stage
COPY --from=build /app/target/release/databench-rs generator

# Set executable permissions for the binary
RUN chmod +x generator

# Set the binary as the default command to run
CMD ["./generator"]
