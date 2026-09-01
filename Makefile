.PHONY: build test infra-up infra-down run-order run-payment

build:
	./gradlew build

test:
	./gradlew test

infra-up:
	docker compose -f demo-stand/docker-compose.yml up -d --wait

infra-down:
	docker compose -f demo-stand/docker-compose.yml down

run-order:
	./gradlew :demo-stand:order-service:bootRun

run-payment:
	./gradlew :demo-stand:payment-service:bootRun
