# KafkaAppender

## 📝 Описание

**KafkaAppender** — кастомный аппендер для Apache Log4j 2, который перенаправляет события логирования в топик Apache Kafka. Это позволяет централизованно собирать логи распределённых Java/Spring-приложений в единую шину сообщений для последующей обработки (ELK, Loki, ClickHouse, Grafana и т.п.).

Проект состоит из двух модулей:

- **`kafka-log4j2-appender`** — библиотека с реализацией аппендера (`MyKafka`), упаковываемая в JAR.
- **`kafka-log4j2-test`** — демонстрационное Spring Boot приложение, использующее аппендер: при обращении к REST-эндпоинту в Kafka публикуются записи разных уровней (`INFO`, `WARN`, `ERROR`).

### Основные возможности

- Регистрация аппендера через стандартный Log4j2 plugin-механизм (`@Plugin(name = "MyKafka")`).
- Настройка топика и произвольных параметров `KafkaProducer` прямо из `log4j2.xml`.
- Поддержка любых Layout'ов Log4j2 (по умолчанию — `PatternLayout`).
- Дефолты для продьюсера: `acks=1`, `retries=3`, сериализация ключа/значения как `String`.

---

## 🛠 Технологический стек

| Категория          | Технология                              |
|--------------------|-----------------------------------------|
| Язык               | Java 17                                 |
| Сборка             | Apache Maven                            |
| Логирование        | Apache Log4j 2 (`log4j-core` 2.20.0)    |
| Брокер сообщений   | Apache Kafka (`kafka-clients` 3.9.1)    |
| Фреймворк (тест)   | Spring Boot 3.5.5 (Web, Log4j2 starter) |
| Тестирование       | JUnit 5 (`spring-boot-starter-test`)    |

---

## 📂 Структура проекта

```
KafkaAppender/
├── kafka-log4j2-appender/                    # Библиотека-аппендер
│   ├── pom.xml
│   └── src/main/java/com/puchkov/log4j2/kafka/
│       └── KafkaAppender.java                # Реализация Log4j2-плагина "MyKafka"
│
├── kafka-log4j2-test/                        # Демонстрационное Spring Boot приложение
│   ├── pom.xml
│   ├── mvnw, mvnw.cmd                        # Maven Wrapper
│   ├── HELP.md
│   └── src/
│       ├── main/
│       │   ├── java/com/puchkov/kafkalog4j2test/
│       │   │   ├── KafkaLog4j2TestApplication.java   # Точка входа Spring Boot
│       │   │   └── controller/
│       │   │       └── TestController.java           # REST-контроллер с эндпоинтом /test
│       │   └── resources/
│       │       ├── application.properties            # Настройки Spring Boot
│       │       └── log4j2.xml                        # Конфигурация логгера + регистрация MyKafka
│       └── test/java/com/puchkov/kafkalog4j2test/
│           └── KafkaLog4j2TestApplicationTests.java  # Тест contextLoads
│
└── README.md
```

> ⚠️ TODO: в корне проекта отсутствует родительский `pom.xml`, агрегирующий оба модуля. Сейчас каждый модуль собирается отдельно.

---

## 📋 Требования

- **JDK 17+**
- **Apache Maven 3.6+** (либо использовать `./mvnw` в `kafka-log4j2-test`)
- **Apache Kafka** (брокер, доступный по адресу `localhost:9092` по умолчанию)
- Существующий топик Kafka `test-logs` (либо включённое автосоздание топиков на брокере)

> 💡 Для локального запуска Kafka можно использовать официальный Docker-образ `apache/kafka` или `confluentinc/cp-kafka`.
> ⚠️ TODO: в проекте отсутствует `docker-compose.yml` для быстрого поднятия Kafka — стоит добавить.

---

## ⚙️ Установка

1. **Склонируйте репозиторий:**
   ```bash
   git clone <url-репозитория>
   cd KafkaAppender
   ```

2. **Соберите и установите модуль-аппендер в локальный Maven-репозиторий:**
   ```bash
   cd kafka-log4j2-appender
   mvn clean install
   ```
   Это положит `kafka-log4j2-appender-1.0-SNAPSHOT.jar` в `~/.m2/repository`, после чего тестовое приложение сможет подтянуть его как зависимость.

3. **Соберите тестовое приложение:**
   ```bash
   cd ../kafka-log4j2-test
   ./mvnw clean package         # Linux/macOS
   mvnw.cmd clean package       # Windows
   ```

---

## 🔧 Конфигурация

### Spring Boot (`application.properties`)

| Параметр                   | Значение по умолчанию    | Описание                                            |
|----------------------------|--------------------------|-----------------------------------------------------|
| `spring.application.name`  | `kafka-log4j2-test`      | Имя приложения                                      |
| `logging.config`           | `classpath:log4j2.xml`   | Путь до конфигурации Log4j2                         |
| `server.port`              | `8080`                   | Порт встроенного веб-сервера                        |

### Аппендер (`log4j2.xml`)

Аппендер `MyKafka` принимает следующие XML-атрибуты:

| Атрибут            | Тип       | Обязательный | Описание                                                    |
|--------------------|-----------|:------------:|-------------------------------------------------------------|
| `name`             | `String`  | да           | Имя аппендера в Log4j2                                      |
| `topic`            | `String`  | да           | Имя Kafka-топика, в который пишутся события                 |
| `ignoreExceptions` | `boolean` | нет          | Подавлять ли исключения при отправке (стандартное поведение)|

Дополнительно через `<Properties>` можно передать **любые свойства** `KafkaProducer` (см. `ProducerConfig`):

```xml
<MyKafka name="Kafka" topic="test-logs">
    <PatternLayout pattern="%d{yyyy-MM-dd HH:mm:ss.SSS} [%t] %-5level %logger{36} - %msg%n"/>
    <Property name="bootstrap.servers">kafka1:9092,kafka2:9092</Property>
    <Property name="acks">all</Property>
    <Property name="retries">5</Property>
    <Property name="compression.type">lz4</Property>
</MyKafka>
```

### Дефолты `KafkaProducer`

Заданы в `KafkaAppender.createProducer(...)` и могут быть переопределены через `<Property>`:

| Свойство            | Значение по умолчанию          |
|---------------------|--------------------------------|
| `bootstrap.servers` | `localhost:9092`               |
| `key.serializer`    | `StringSerializer`             |
| `value.serializer` | `StringSerializer`             |
| `acks`              | `1`                            |
| `retries`           | `3`                            |

> ⚠️ TODO: переменные окружения / `.env.example` отсутствуют — конфигурация целиком через `log4j2.xml` и `application.properties`. При необходимости стоит вынести `bootstrap.servers` в переменную окружения и подставлять через `${env:KAFKA_BOOTSTRAP}`.

---

## 🚀 Запуск

### Режим разработки

```bash
cd kafka-log4j2-test
./mvnw spring-boot:run
```

После старта приложение слушает `http://localhost:8080`.

### Production режим

Сборка исполняемого fat-JAR:
```bash
cd kafka-log4j2-test
./mvnw clean package
java -jar target/kafka-log4j2-test-0.0.1-SNAPSHOT.jar
```

### Проверка работы

```bash
curl http://localhost:8080/test
```

После запроса в топик `test-logs` уйдёт несколько записей разных уровней. Прочитать их можно, например, через `kafka-console-consumer`:
```bash
kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic test-logs \
  --from-beginning
```

### Docker

> ⚠️ TODO: `Dockerfile` и `docker-compose.yml` в проекте отсутствуют — контейнеризация не настроена.

---

## 📡 API Документация

Базовый URL: `http://localhost:8080`

Аутентификация: **отсутствует** (приложение чисто демонстрационное).

### Resource: Test

#### `GET /test` — отправка тестовых логов в Kafka

Метод записывает три события (`INFO`, `WARN`, `ERROR`) через SLF4J. Так как Log4j2 настроен с `MyKafka`-аппендером, эти события попадут в топик `test-logs`.

| Параметр | Расположение | Тип | Обязательный | Описание |
|----------|--------------|-----|:------------:|----------|
| —        | —            | —   | —            | Параметры отсутствуют |

**Тело запроса:** отсутствует.

**Пример запроса:**
```bash
curl -X GET http://localhost:8080/test
```

**Пример ответа** (`200 OK`, `text/plain;charset=UTF-8`):
```
Логи отправлены в Kafka! Проверьте топик 'test-logs'
```

**Сообщения, попадающие в Kafka (топик `test-logs`):**
```
2026-05-04 12:34:56.789 [http-nio-8080-exec-1] INFO  c.p.k.controller.TestController - Тестовое информационное сообщение
2026-05-04 12:34:56.790 [http-nio-8080-exec-1] WARN  c.p.k.controller.TestController - Тестовое предупреждение
2026-05-04 12:34:56.791 [http-nio-8080-exec-1] ERROR c.p.k.controller.TestController - Тестовая ошибка
```

**Возможные коды ответа:**

| Код  | Описание                                                       |
|------|----------------------------------------------------------------|
| 200  | Успех — логи отправлены                                        |
| 404  | Запрошен несуществующий путь (Spring Boot default error page)  |
| 500  | Внутренняя ошибка сервера                                      |

> ℹ️ Если Kafka недоступна, `KafkaProducer.send(...)` вернётся асинхронно, а ошибки попадут во внутренний логгер Log4j2 (`StatusLogger`) — HTTP-ответ `200` всё равно вернётся. Это поведение определяется атрибутом `ignoreExceptions` аппендера.

> ⚠️ TODO: в проекте нет других REST-эндпоинтов, GraphQL/gRPC схем, OpenAPI-спецификации (Swagger/Springdoc). При расширении API стоит подключить `springdoc-openapi-starter-webmvc-ui`.

---

## 🧪 Тестирование

В модуле `kafka-log4j2-test` есть один JUnit-тест (`KafkaLog4j2TestApplicationTests.contextLoads`) — проверяет, что Spring-контекст поднимается.

Запуск тестов:
```bash
cd kafka-log4j2-test
./mvnw test
```

> ⚠️ TODO: модульных тестов на сам `KafkaAppender` (модуль `kafka-log4j2-appender`) нет. Стоит добавить тесты с `MockProducer` из `kafka-clients` и/или интеграционные тесты на `Testcontainers` с реальным Kafka-брокером.

---