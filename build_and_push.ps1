docker buildx build -t ghcr.io/united-manufacturing-hub/databench:latest -f .\golang\Dockerfile .
docker push ghcr.io/united-manufacturing-hub/databench:latest-golang

docker buildx build -t ghcr.io/united-manufacturing-hub/databench:latest -f .\rust\Dockerfile .
docker push ghcr.io/united-manufacturing-hub/databench:latest-rust

git add .
git commit -m "feat: updated image"
git push
