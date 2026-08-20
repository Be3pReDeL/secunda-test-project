# Асинхронный сервис процессинга платежей

Сервис принимает платежи по HTTP, атомарно сохраняет платеж и outbox-событие в
PostgreSQL, публикует событие в RabbitMQ и асинхронно эмулирует обработку платежа.
После перехода платежа в `succeeded` или `failed` consumer отправляет webhook.

## Запуск в Docker

Требуются Docker и Docker Compose.

```bash
cp .env.example .env
docker compose up --build -d
docker compose ps
```

Compose запускает PostgreSQL, RabbitMQ, одноразовую миграцию Alembic, API и
consumer. API и consumer стартуют только после успешной миграции и готовности
зависимых сервисов.

- API: `http://localhost:8000`
- RabbitMQ Management: `http://localhost:15672` (`payments` / `payments`)
- PostgreSQL: `localhost:5432` (`payments` / `payments`)

Логи и остановка окружения:

```bash
docker compose logs -f api consumer
docker compose down
```

Для повторного применения миграций вручную:

```bash
docker compose run --rm migrate
```

## Переменные окружения

Основные настройки находятся в `.env.example` и имеют префикс `PAYMENTS_`.

| Переменная | Назначение | Значение по умолчанию |
| --- | --- | --- |
| `PAYMENTS_API_KEY` | Статический ключ для `X-API-Key`, минимум 16 символов | обязательна |
| `PAYMENTS_DATABASE_URL` | URL PostgreSQL с asyncpg | обязательна |
| `PAYMENTS_RABBITMQ_URL` | AMQP URL RabbitMQ | обязательна |
| `PAYMENTS_ALLOW_PRIVATE_WEBHOOKS` | Разрешить private/loopback webhook-адреса только для локальной разработки | `false` |
| `PAYMENTS_GATEWAY_MIN_DELAY_SECONDS` | Минимальная задержка эмулятора шлюза | `2.0` |
| `PAYMENTS_GATEWAY_MAX_DELAY_SECONDS` | Максимальная задержка эмулятора шлюза | `5.0` |
| `PAYMENTS_GATEWAY_SUCCESS_PROBABILITY` | Вероятность успешного платежа | `0.9` |
| `PAYMENTS_RETRY_BASE_DELAY_SECONDS` | База экспоненциальной задержки retry | `1.0` |
| `PAYMENTS_WEBHOOK_TIMEOUT_SECONDS` | Таймаут webhook-запроса | `5.0` |
| `PAYMENTS_BROKER_PUBLISH_TIMEOUT_SECONDS` | Таймаут подтверждения публикации | `5.0` |
| `PAYMENTS_OUTBOX_BATCH_SIZE` | Максимальный размер пачки relay | `100` |
| `PAYMENTS_OUTBOX_POLL_INTERVAL_SECONDS` | Интервал опроса outbox | `0.5` |
| `PAYMENTS_OUTBOX_LEASE_SECONDS` | Срок аренды outbox-записи | `30.0` |
| `PAYMENTS_OUTBOX_MAX_BACKOFF_SECONDS` | Верхняя граница backoff relay | `60.0` |

Перед использованием вне локального окружения замените API-ключ и учетные данные
PostgreSQL/RabbitMQ. Не включайте `PAYMENTS_ALLOW_PRIVATE_WEBHOOKS=true` в
недоверенном окружении.

## Примеры API

Все эндпоинты, включая `/health`, требуют заголовок `X-API-Key`. Встроенные
Swagger/ReDoc отключены.

```bash
export PAYMENT_API_KEY='replace-with-at-least-32-random-characters'
```

Создание платежа:

```bash
curl --include --request POST 'http://localhost:8000/api/v1/payments' \
  --header "X-API-Key: ${PAYMENT_API_KEY}" \
  --header 'Idempotency-Key: demo-payment-001' \
  --header 'Content-Type: application/json' \
  --data '{
    "amount": "1490.00",
    "currency": "RUB",
    "description": "Оплата заказа 42",
    "metadata": {"order_id": "42"},
    "webhook_url": "https://webhook.site/replace-with-your-id"
  }'
```

Успешный ответ имеет статус `202 Accepted`. Сохраните `payment_id` из ответа и
получите текущее состояние:

```bash
export PAYMENT_ID='replace-with-payment-id'

curl --include \
  --header "X-API-Key: ${PAYMENT_API_KEY}" \
  "http://localhost:8000/api/v1/payments/${PAYMENT_ID}"
```

Повтор идентичного POST с тем же `Idempotency-Key` возвращает тот же платеж.
Повтор ключа с другим телом возвращает `409 Conflict`. Отсутствующий или неверный
`X-API-Key` возвращает `401 Unauthorized`.

Для локального webhook-сервера внутри private-сети Docker явно установите
`PAYMENTS_ALLOW_PRIVATE_WEBHOOKS=true` в `.env` и пересоздайте `api` и `consumer`.

## Архитектурные гарантии

- Payment и outbox-событие создаются в одной транзакции PostgreSQL. Уникальный
  `idempotency_key` и fingerprint тела защищают от дублей и конфликтного повторного
  использования ключа.
- Outbox relay работает в API lifespan, выбирает записи через
  `FOR UPDATE SKIP LOCKED`, использует ограниченную lease и отмечает событие
  опубликованным только после publisher confirm RabbitMQ.
- Доставка события имеет семантику at-least-once: после сбоя между broker confirm
  и фиксацией `published_at` возможен дубль с тем же `event_id`. Consumer повторно
  не запускает шлюз для терминального платежа.
- Эмулятор шлюза детерминирован по `payment_id`: задержка находится в диапазоне
  2-5 секунд, а результат соответствует заданной вероятности успеха.
- Webhook отправляется без redirects, с ограниченным таймаутом и защитой от SSRF.
  Заголовки `Idempotency-Key` и `X-Webhook-Event-ID` содержат стабильный `event_id`,
  поэтому получатель может дедуплицировать повторную доставку.
- Ошибка webhook после первой и второй попытки направляет сообщение в отдельную
  TTL retry-очередь с задержками 1 и 2 секунды. После третьей неуспешной попытки
  RabbitMQ помещает сообщение в `payments.dlq` через `payments.dlx`.
- Ошибки публикации outbox не ограничены тремя попытками: relay повторяет их с
  ограниченным экспоненциальным backoff, сохраняя событие в PostgreSQL.

## Локальная разработка и тесты

Требуется Python 3.12.

```bash
python3.12 -m venv .venv
source .venv/bin/activate
python -m pip install --editable '.[dev]'
```

Проверки проекта:

```bash
ruff check .
mypy src
pytest
pytest --cov=payment_service --cov-report=term-missing
```

Для ручного запуска компонентов вне Docker поднимите зависимости и задайте URL с
`localhost`, затем примените миграцию:

```bash
docker compose up -d postgres rabbitmq
export PAYMENTS_API_KEY='replace-with-at-least-32-random-characters'
export PAYMENTS_DATABASE_URL='postgresql+asyncpg://payments:payments@localhost:5432/payments'
export PAYMENTS_RABBITMQ_URL='amqp://payments:payments@localhost:5672/'
alembic upgrade head
uvicorn payment_service.main:app --host 0.0.0.0 --port 8000
```

Consumer запускается в отдельном терминале с теми же переменными окружения:

```bash
faststream run payment_service.worker:app
```
