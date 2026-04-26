# USA AUTO PARTS

Сайт складу та вітрини USA AUTO PARTS.

## Portainer

Основна інструкція запуску:

- `PORTAINER_DEPLOY.md`

Для Portainer рекомендовано використовувати:

- `docker-compose.vps.yml`, якщо Portainer має доступ до репозиторію і може зібрати образ;
- `stack.yml`, якщо образ `usa-auto-parts-site:vps-latest` вже зібраний або завантажений у Docker registry.

## Дані, які не зберігаються в Git

- `app/uploads/` — фото та завантаження;
- `backups/` — архіви сайту;
- `rclone/` — конфіг Google Drive;
- `.env` — приватні паролі та ключі.
