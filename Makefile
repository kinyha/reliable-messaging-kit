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

.PHONY: demo-reset demo-reconcile demo-kill9 demo-kill9-naive demo-kafka-down demo-three-relays demo-rollback demo-replay

demo-reset:
	demo-stand/scripts/reset.sh

demo-reconcile:
	demo-stand/scripts/reconcile.sh

demo-kill9:
	demo-stand/scripts/scenario-kill9.sh outbox

demo-kill9-naive:
	demo-stand/scripts/scenario-kill9.sh naive

demo-kafka-down:
	demo-stand/scripts/scenario-kafka-down.sh

demo-three-relays:
	demo-stand/scripts/scenario-three-relays.sh

demo-rollback:
	demo-stand/scripts/scenario-rollback.sh

demo-replay:
	demo-stand/scripts/scenario-replay.sh
