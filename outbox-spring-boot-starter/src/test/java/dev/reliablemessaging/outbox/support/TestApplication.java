package dev.reliablemessaging.outbox.support;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
// Пустое Spring Boot приложение. Стартер подключается к нему сам, через автоконфигурацию, ровно так же,
// как к приложению пользователя библиотеки.
// Рядом лежит src/test/resources/db/migration/V1__app.sql: «бизнес-таблица» effects, которую тесты
// используют вместо orders/payments. Заодно это миграция V1 самого приложения, и SchemaMigrationIT
// проверяет, что она не конфликтует с V1 стартера.
@SpringBootConfiguration
@EnableAutoConfiguration
public class TestApplication { }
