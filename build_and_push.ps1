docker buildx build -t ghcr.io/united-manufacturing-hub/databench:latest -f .\Dockerfile .
docker push ghcr.io/united-manufacturing-hub/databench:latest
git add .
git commit -m "feat: updated image"
git push
