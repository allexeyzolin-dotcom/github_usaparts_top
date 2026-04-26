# USA AUTO PARTS: запуск у Portainer

## 1. Підготовка

У Portainer відкрийте **Stacks → Add stack**.

Рекомендований варіант:

- якщо код лежить на VPS або в Git, використовуйте `docker-compose.vps.yml`;
- якщо образ уже зібраний або завантажений у registry, використовуйте `stack.yml`.

Для запуску через `stack.yml` образ `WEB_IMAGE` має існувати на VPS або в Docker registry.

## 2. Env для Stack

Скопіюйте значення зі `stack.env.example` і змініть мінімум:

```env
POSTGRES_PASSWORD=your-strong-password
SECRET_KEY=your-long-random-secret
APP_PORT=8080
WEB_IMAGE=usa-auto-parts-site:vps-latest
```

## 3. Google Drive backup

Сайт робить локальний повний бекап у volume `backups`.

Для синхронізації з Google Drive у контейнері використовується `rclone`.
Один раз налаштуйте remote:

```bash
docker exec -it <web-container-name> rclone config
```

Створіть remote, наприклад `gdrive`, тип `drive`.
Після цього в кабінеті сайту відкрийте **Бекап** і вкажіть:

```text
gdrive:USA_AUTO_PARTS_BACKUPS
```

або задайте в env:

```env
BACKUP_SYNC_ENABLED=1
BACKUP_RCLONE_REMOTE=gdrive:USA_AUTO_PARTS_BACKUPS
```

## 4. Що входить у повний бекап

- PostgreSQL dump у форматі `pg_restore`;
- `app/uploads` з усіма фото;
- CSV `all_products.csv` для швидкого перегляду бази "Всі товари";
- CSV `warehouse_inventory.csv` для складів;
- JSON копії всіх таблиць;
- вихідні файли сайту без тимчасового сміття.

## 5. Перевірка після запуску

Відкрийте:

```text
http://SERVER_IP:APP_PORT/
```

У кабінеті перевірте вкладку **Бекап** і створіть перший ручний архів.
