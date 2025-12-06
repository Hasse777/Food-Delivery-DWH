# 🧩 DWH-for-FoodTech

## 📘 Описание проекта

Система расчётов с ресторанами и курьерами в FoodTech-сервисе.  
Данные собираются из разных источников (**MongoDB, PostgreSQL, API**), загружаются в хранилище PostgreSQL и проходят через слои **STG**, **DDS** и итоговый слой **CDM**.

Итог проекта — две витрины:

* **`cdm.dm_settlement_report`** — расчёты с ресторанами по дням
* **`cdm.dm_courier_ledger`** — расчёты с курьерами по месяцам

---

## ⚙️ Архитектура пайплайна

**Источники данных:**
* MongoDB — заказы, рестораны  
* PostgreSQL — бонусные операции  
* API /couriers, /deliveries — курьеры, рейтинги, чаевые

**Слои хранилища:**
* STG — загрузка сырых данных
* DDS — нормализованные сущности (снежинка)
* CDM — расчётные витрины

**Оркестрация:** Apache Airflow  
DAG-и выполняют:
* Загрузку данных из **MongoDB, PostgreSQL и внешнего API** в слой STG
* Формирование нормализованных сущностей (DDS)
* Расчёт итоговых витрин в CDM

---

## 📊 Итоговые витрины

### `cdm.dm_courier_ledger` — выплаты курьерам (по месяцам)

| Поле | Описание |
|------|----------|
| `id` | идентификатор записи |
| `courier_id` | курьер |
| `courier_name` | ФИО курьера|
| `settlement_year` | год расчёта |
| `settlement_month` | месяц расчёта |
| `orders_count` | количество заказов |
| `orders_total_sum` | сумма заказов |
| `rate_avg` | средний рейтинг курьера по оценкам пользователей|
| `order_processing_fee` | комиссия компании |
| `courier_order_sum` | фиксированные выплаты курьеру |
| `courier_tips_sum` | чаевые |
| `courier_reward_sum` | итоговая выплата курьеру |

**Пример данных:**

![dm_courier_ledger](img/cdm.dm_courier_ledger.png)

---

### `cdm.dm_settlement_report` — выплаты ресторанам (по дням)

| Поле | Описание |
|------|----------|
| `id` | идентификатор записи |
| `restaurant_id` | ресторан |
| `restaurant_name` | название ресторана |
| `settlement_date` | дата расчёта |
| `orders_count` | количество заказов |
| `orders_total_sum` | стоимость заказов |
| `orders_bonus_payment_sum` | оплата бонусами |
| `orders_bonus_granted_sum` | начисленные бонусы |
| `order_processing_fee` | комиссия компании |
| `restaurant_reward_sum` | выплата ресторану |

**Пример данных:**

![dm_settlement_report](img/cdm.dm_settlement_report.png)

---

## 🧠 Технологический стек

* **PostgreSQL** — хранилище данных  
* **MongoDB** — источник заказов  
* **Airflow** — управление пайплайнами  
* **Docker Compose** — инфраструктура  
* **Python / SQL** — загрузка и расчёты

---

## 🚀 Результат

* Автоматизированный DWH-пайплайн
* Интеграция разных источников данных
* Ежедневные и ежемесячные финансовые отчёты
* Готовые суммы выплат для бизнеса

---

## 🖼 Визуализация

**DWH архитектура (слои STG/DDS/CDM)**  
![DWH](img/cdm_schema.png)

**STG модель (сырые данные API)**  
![STG](img/stg_schema.png)

**DDS модель (нормализованные сущности)**  
![DDS](img/dds_schema.png)

**Airflow граф пайплайна**  
![Airflow](img/airflow_tasks.png)
