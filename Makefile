.PHONY: run-example

run-example:
	@docker compose --file docs/samples/mysql-binlog-watch-row/docker-compose.yml up -d --wait
	@docker exec -i mysql-binlog-watch-row mysql -uroot -pMiContrasenaSegura -e " \
		CREATE DATABASE IF NOT EXISTS test; \
		USE test; \
		CREATE TABLE IF NOT EXISTS test (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(100));"
	@go run cmd/main.go watch --config docs/samples/mysql-binlog-watch-row/config.yaml & echo $$! > /tmp/go_pid.tmp
	@sleep 2
	@echo "INSERT new row..."
	@docker exec -i mysql-binlog-watch-row mysql -uroot -pMiContrasenaSegura -e " \
		USE test; \
		INSERT INTO test (name) VALUES ('Example');"
	@echo "UPDATE the row..."
	@docker exec -i mysql-binlog-watch-row mysql -uroot -pMiContrasenaSegura -e " \
		USE test; \
		UPDATE test SET name='Modified' WHERE id=1;"
	@echo "DELETE the row..."
	@docker exec -i mysql-binlog-watch-row mysql -uroot -pMiContrasenaSegura -e " \
		USE test; \
		DELETE FROM test WHERE id=1;"
	@kill `cat /tmp/go_pid.tmp`
	@rm -f /tmp/go_pid.tmp
	@docker compose --file docs/samples/mysql-binlog-watch-row/docker-compose.yml down --volumes --remove-orphans

