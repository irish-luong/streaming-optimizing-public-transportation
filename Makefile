init:
    docker build . -t streaming-app
up:
    docker-compose up -d
down:
    docker-compose down --remove-orphans