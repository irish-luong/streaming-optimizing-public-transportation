init:
	docker build . -t iris-luong/streaming-app:latest
up:
	docker-compose up -d

build:
	make init
	make up
down:
	docker-compose down --remove-orphans