docker buildx build -t ghcr.io/united-manufacturing-hub/databench:latest-rust -f .\databench-rs\Dockerfile .\databench-rs\
docker push ghcr.io/united-manufacturing-hub/databench:latest-rust

git add .
git commit -m "feat: updated image"
git push
