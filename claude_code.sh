docker build -t claude-code-image .
docker run -it -u $(id -u):$(id -g) -v $(pwd):/MBD claude-code-image


