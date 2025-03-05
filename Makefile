.PHONY: run-example

run-example:
	@docker compose --file docs/samples/mysql-binlog-watch-row/docker-compose.yml up -d --wait
	@docker exec -i mysql-binlog-watch-row mysql -uroot -pMiContrasenaSegura -e " \
		CREATE DATABASE IF NOT EXISTS test; \
		USE test; \
		CREATE TABLE IF NOT EXISTS test (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(100));"

	@(HOSTNAME=test go run cmd/main.go watch --config docs/samples/mysql-binlog-watch-row/config.yaml & echo $$! > /tmp/go_pid.tmp)
	@(HOSTNAME=test2 go run cmd/main.go watch --config docs/samples/mysql-binlog-watch-row/config.yaml & echo $$! > /tmp/go_pid.tmp)

	@sleep 6
	@for i in $$(seq 1 20); do \
		echo "INSERT new row $$i..."; \
		docker exec -i mysql-binlog-watch-row mysql -uroot -pMiContrasenaSegura -e " \
			USE test; \
			INSERT INTO test (name) VALUES ('Example_$$i');" ; \
		echo "UPDATE the row $$i..."; \
		docker exec -i mysql-binlog-watch-row mysql -uroot -pMiContrasenaSegura -e " \
			USE test; \
			UPDATE test SET name='Modified_$$i' WHERE id=$$i;" ; \
		echo "DELETE the row $$i..."; \
		docker exec -i mysql-binlog-watch-row mysql -uroot -pMiContrasenaSegura -e " \
			USE test; \
			DELETE FROM test WHERE id=$$i;" ; \
	done

	@sleep 5
	@if [ -f /tmp/go_pid.tmp ]; then \
		kill -9 `cat /tmp/go_pid.tmp` && rm -f /tmp/go_pid.tmp; \
	fi; \
	ps aux | grep '[g]o-build' | cut -f2 -w | xargs -r kill -9 || true

	@docker compose --file docs/samples/mysql-binlog-watch-row/docker-compose.yml down --volumes --remove-orphans


