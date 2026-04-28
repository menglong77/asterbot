# astrbot_plugin_pg_memory

PostgreSQL + pgvector memory plugin for AstrBot.

It provides:

- Passive QQ group message archiving
- Long-term memory with vector search
- Group chat analysis reports
- Optional FastAPI admin panel
- Image report delivery fallbacks for NapCat/OneBot

## Requirements

- AstrBot `>= 4.16.0`
- PostgreSQL with `pgvector`
- Python packages from `requirements.txt`
- An AstrBot embedding provider if vector memory is enabled

## Install

Copy this directory to AstrBot's plugin directory:

```text
AstrBot/data/plugins/astrbot_plugin_pg_memory
```

Install dependencies in the AstrBot environment:

```bash
pip install -r requirements.txt
```

Create plugin config from `config.example.json`, then set:

- `database.password`
- `super_admins`
- `embedding_provider_id`
- `admin_panel.token`

Do not reuse the example password or token in production.

## Commands

- `~归档`
- `~归档 状态`
- `~归档 停止`
- `~归档 导出`
- `~memory remember <content>`
- `~memory list_records`
- `~memory list`
- `~memory delete_record <id> confirm`
- `~memory delete_session_memory <session_id> confirm`
- `~memory reset confirm`
- `~memory get_session_id`
- `~memory init`
- `~memory sync`
- `~群分析`
- `~group_analysis`

## Security

This repository intentionally contains only example configuration. Never commit:

- Real QQ IDs if you consider them private
- API keys or model provider tokens
- Cloudflare Tunnel tokens
- New API tokens
- AstrBot runtime data, logs, archives, or database dumps

## License

MIT
