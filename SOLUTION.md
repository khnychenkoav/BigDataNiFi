# Решение лабораторной работы №5

## Что сделано

В работе собран потоковый контур на Kafka, Apache NiFi, PostgreSQL и Metabase. Producer читает все CSV-файлы, преобразует каждую строку в JSON и отправляет сообщения в Kafka topic pet_sales_raw. NiFi автоматически настраивается через REST API, читает этот topic процессором ConsumeKafka_2_6 и пишет сырые записи в PostgreSQL через PutDatabaseRecord

В PostgreSQL данные сохраняются без агрегаций в stage.sales_raw. После загрузки SQL-скрипт строит шесть отчетных таблиц в схеме reports. Metabase поднимается отдельным контейнером и подключается к этой же PostgreSQL-базе для визуализации отчетов

## Запуск

Полный запуск выполняется командой .\scripts\run_all.ps1

Скрипт собирает локальные образы, поднимает PostgreSQL, Kafka, NiFi и Metabase, создает flow в NiFi, отправляет 10000 сообщений в Kafka, ждет загрузку всех строк в PostgreSQL и строит отчеты

Если нужно пересоздать окружение полностью, можно выполнить:

docker compose down -v
.\scripts\run_all.ps1

Проверка результата выполняется командой .\scripts\validate.ps1

## Подключения

PostgreSQL:

- Host localhost
- Port 5436
- Database nifi_lab
- User lab
- Password lab

Kafka:

- Bootstrap server localhost:9095
- Internal bootstrap server kafka:9092
- Topic pet_sales_raw

NiFi:

- Web UI http://localhost:8083/nifi
- Flow создается сервисом nifi-init
- Основной маршрут ConsumeKafka_2_6 -> PutDatabaseRecord

Metabase:

- Web UI http://localhost:3002
- Для подключения внутри Docker network использовать host postgres и port 5432
- Database nifi_lab
- User lab
- Password lab

## Как устроен NiFi flow

Сервис nifi-init запускает скрипт nifi/configure_flow.py и через NiFi REST API создает два controller service и два processor

PostgreSQL connection pool подключает NiFi к PostgreSQL через JDBC-драйвер, который добавлен в образ NiFi. JSON sales reader читает JSON-сообщения и выводит record-структуру. Consume sales JSON from Kafka читает topic pet_sales_raw с offset earliest. Write raw sales to PostgreSQL записывает записи в таблицу stage.sales_raw

Такой вариант оставляет решение проверяемым в UI NiFi, но не требует вручную собирать canvas при каждом запуске

## SQL-отчеты

Отчеты строятся скриптом sql/postgres/01_build_reports.sql

Создаются таблицы:

- reports.report_sales_by_product для анализа продуктов, категорий, выручки, рейтинга и топ-10 по проданным единицам
- reports.report_sales_by_customer для анализа покупателей, стран, среднего чека и топ-10 клиентов по сумме покупок
- reports.report_sales_by_time для месячных трендов, среднего заказа и сравнения выручки с предыдущим месяцем
- reports.report_sales_by_store для анализа магазинов, городов, стран и топ-5 магазинов по выручке
- reports.report_sales_by_supplier для анализа поставщиков, стран, средней цены товара и топ-5 поставщиков по выручке
- reports.report_product_quality для анализа рейтингов, отзывов и корреляции рейтинга с продажами

## Проверка

Ожидаемый результат после .\scripts\validate.ps1:

- stage.sales_raw содержит 10000 строк
- каждый из 10 CSV-файлов дает по 1000 строк
- все шесть отчетных таблиц заполнены
- report_sales_by_time содержит 12 месячных периодов
- top-10 и top-5 флаги отмечают ровно нужное количество строк
- sale_total_price сохраняется как исходная сумма, а рядом рассчитывается product_price * sale_quantity и флаг качества is_total_consistent

## Визуализация в Metabase

После запуска нужно открыть http://localhost:3002, пройти первичную настройку Metabase и добавить PostgreSQL datasource

Параметры подключения из Metabase:

- Host postgres
- Port 5432
- Database nifi_lab
- User lab
- Password lab

Для визуализации можно строить вопросы или SQL-запросы поверх таблиц reports.report_sales_by_product, reports.report_sales_by_customer, reports.report_sales_by_time, reports.report_sales_by_store, reports.report_sales_by_supplier и reports.report_product_quality
