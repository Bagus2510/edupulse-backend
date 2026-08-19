# ⚡ Datapulse Backend

<div align="center">

![FastAPI](https://img.shields.io/badge/FastAPI-009688?style=for-the-badge&logo=fastapi&logoColor=white)
![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-4169E1?style=for-the-badge&logo=postgresql&logoColor=white)
![SQLAlchemy](https://img.shields.io/badge/SQLAlchemy-D71F00?style=for-the-badge&logo=sqlalchemy&logoColor=white)
![Pydantic](https://img.shields.io/badge/Pydantic-E92063?style=for-the-badge&logo=pydantic&logoColor=white)

**FastAPI backend untuk platform analitik pendidikan — REST API, auth, AI chat, pipeline management**

[API Docs](#-api-endpoints) • [Setup](#-installation--setup) • [Services](#-services) • [Database](#-database-tables)

</div>

---

## 📖 About The Project

Backend FastAPI yang menyediakan seluruh logic untuk platform Datapulse — mulai dari pipeline ETL, data quality checks, BI dashboard embedding, AI-powered analytics, hingga user management dengan RBAC.

### 🎯 Key Features

- 🔐 **JWT Authentication** — Access token + refresh token dengan bcrypt password hashing
- 👥 **RBAC** — 3 role (Viewer, Editor, Admin) dengan dependency injection
- ⚙️ **Pipeline Management** — CRUD, run, cancel, DAG generation untuk Airflow
- 📊 **Data Quality** — Built-in checks + configurable custom rules per dataset
- 🤖 **AI Chat** — Natural language query via Gemini dengan evidence persistence
- 📈 **Superset Integration** — Guest token generation untuk embedded dashboard

---

## 💡 Architecture

```
┌─────────────┐     ┌──────────────┐     ┌──────────────┐
│  PostgreSQL  │◄────│   FastAPI    │────►│   Next.js    │
│  (raw/mart)  │     │  (backend)   │     │  (frontend)  │
└─────────────┘     └──────────────┘     └──────────────┘
       ▲                   ▲
       │                   │
       ▼                   ▼
┌─────────────┐     ┌──────────────┐
│   Airflow    │     │   Superset   │
│ (orchestrate)│     │   (BI/ embed)│
└─────────────┘     └──────────────┘
```

---

## 🛠️ Technologies Used

- **Python 3.11+**
- **FastAPI** — Web framework
- **SQLAlchemy 2** — Async ORM
- **asyncpg** — PostgreSQL async driver
- **Pydantic v2** — Data validation
- **python-jose** — JWT tokens
- **bcrypt** — Password hashing
- **LangChain** — AI orchestration
- **Google Gemini 2.5 Flash** — LLM for chat
- **httpx** — Async HTTP client

---

## 🚀 Installation & Setup

### Prerequisites
- Python 3.11+
- PostgreSQL 15+

### Installation Steps

1. **Clone the repository**
```bash
git clone https://github.com/Bagus2510/datapulse.git
cd datapulse/backend
```

2. **Create virtual environment**
```bash
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate
```

3. **Install dependencies**
```bash
pip install -r requirements.txt
```

4. **Configure environment**
```bash
cp .env.example .env
```

Edit `.env` with your credentials:
```env
DB_HOST=localhost
DB_PORT=5432
DB_USER=postgres
DB_PASSWORD=your-password
DB_NAME=datapulse
GEMINI_API_KEY=your-gemini-key
SECRET_KEY=your-secret-key
```

5. **Run server**
```bash
uvicorn app.main:app --reload --port 8000
```

API docs: **http://localhost:8000/docs**

---

## 🗂️ Project Structure

```
backend/
│
├── app/
│   ├── core/
│   │   ├── database.py           # SQLAlchemy engine & session
│   │   ├── pg_pool.py            # Shared asyncpg pools
│   │   ├── http_client.py        # Shared Airflow HTTP client + timeout
│   │   └── security.py           # JWT, RBAC, password hashing
│   │
│   ├── models/
│   │   └── schemas.py            # Pydantic schemas
│   │
│   ├── routers/
│   │   ├── auth.py               # Login, refresh token
│   │   ├── pipelines.py          # Pipeline CRUD + run
│   │   ├── pipeline.py           # Pipeline detail (DAG-based)
│   │   ├── datasets.py           # Dataset browse + quality
│   │   ├── quality_rules.py      # Custom quality rules CRUD
│   │   ├── dashboards.py         # Dashboard + dependencies
│   │   ├── domains.py            # Domain management
│   │   ├── ai.py                 # AI chat endpoints
│   │   ├── admin_users.py        # User management
│   │   ├── home.py               # Home overview
│   │   ├── lineage.py            # Data lineage
│   │   ├── activity.py           # Activity log
│   │   ├── settings.py           # System settings
│   │   ├── superset.py           # Superset guest token
│   │   ├── airflow.py            # Airflow integration
│   │   └── metadata.py           # Metadata catalog
│   │
│   ├── services/
│   │   ├── dag_generator.py      # Generate Airflow DAGs
│   │   ├── airflow_client.py     # Airflow REST API client
│   │   ├── data_quality.py       # Quality check runner
│   │   ├── ai_chat.py            # AI chat + evidence
│   │   ├── gemini_client.py      # Gemini API wrapper
│   │   ├── superset_client.py    # Superset API client
│   │   ├── superset_data.py      # Superset data queries
│   │   ├── mart_metadata.py      # Mart table registration
│   │   ├── pipeline_validator.py # Step validation
│   │   └── cron_parser.py        # Cron → human-readable
│   │
│   └── main.py                   # App startup, tables
│
├── sql/                          # SQL seeds & migrations
├── requirements.txt
├── .env.example
└── README.md
```

---

## 📡 API Endpoints

### Auth
| Method | Endpoint | Auth | Description |
|--------|----------|------|-------------|
| POST | `/api/auth/login` | No | Login (OAuth2 form) |
| POST | `/api/auth/refresh` | No | Refresh token |

### Pipelines
| Method | Endpoint | Auth | Description |
|--------|----------|------|-------------|
| GET | `/api/pipelines` | Viewer+ | List pipelines |
| POST | `/api/pipelines` | Editor+ | Create pipeline |
| GET | `/api/pipelines/{id}` | Viewer+ | Get pipeline detail |
| PUT | `/api/pipelines/{id}` | Editor+ | Update pipeline |
| DELETE | `/api/pipelines/{id}` | Admin | Delete pipeline |
| POST | `/api/pipelines/{id}/run` | Editor+ | Run pipeline |
| POST | `/api/pipelines/{id}/cancel` | Editor+ | Cancel pipeline |
| GET | `/api/pipelines/{id}/status` | Viewer+ | Get run status |
| GET | `/api/pipelines/{id}/runs` | Viewer+ | Get run history |
| PATCH | `/api/pipelines/{id}/toggle-active` | Editor+ | Toggle active |

### Datasets
| Method | Endpoint | Auth | Description |
|--------|----------|------|-------------|
| GET | `/api/datasets` | Viewer+ | List all datasets |
| GET | `/api/datasets/{schema}.{table}/info` | Viewer+ | Table info |
| GET | `/api/datasets/{schema}.{table}/quality` | Viewer+ | Quality check |

### Quality Rules
| Method | Endpoint | Auth | Description |
|--------|----------|------|-------------|
| GET | `/api/quality-rules/asset/{id}` | Viewer+ | List rules |
| POST | `/api/quality-rules` | Editor+ | Create rule |
| PUT | `/api/quality-rules/{id}` | Editor+ | Update rule |
| DELETE | `/api/quality-rules/{id}` | Editor+ | Delete rule |

### Dashboards
| Method | Endpoint | Auth | Description |
|--------|----------|------|-------------|
| GET | `/api/dashboards` | Viewer+ | List dashboards |
| POST | `/api/dashboards` | Editor+ | Create dashboard |
| PUT | `/api/dashboards/{id}` | Editor+ | Update dashboard |
| DELETE | `/api/dashboards/{id}` | Admin | Delete dashboard |
| GET | `/api/dashboards/{id}/dependencies` | Viewer+ | List dependencies |
| POST | `/api/dashboards/{id}/dependencies` | Editor+ | Add dependency |
| DELETE | `/api/dashboards/{id}/dependencies/{table}` | Editor+ | Remove dependency |
| GET | `/api/dashboards/{id}/dependencies/validate` | Viewer+ | Validate dependencies |

### AI Analytics
| Method | Endpoint | Auth | Description |
|--------|----------|------|-------------|
| POST | `/api/ai/analyze` | Viewer+ | Structured dashboard analysis |
| GET | `/api/ai/chat/sessions` | Viewer+ | List owned chat sessions |
| POST | `/api/ai/chat` | Viewer+ | Chat (non-streaming) |
| POST | `/api/ai/chat/stream` | Viewer+ | Chat dengan SSE evidence |
| GET | `/api/ai/chat/history/{session_id}` | Viewer+ | Get owned session history |
| POST | `/api/ai/chat/clear` | Viewer+ | Clear owned session messages |
| DELETE | `/api/ai/chat/sessions/{session_id}` | Viewer+ | Delete owned session |

### Admin
| Method | Endpoint | Auth | Description |
|--------|----------|------|-------------|
| GET | `/api/admin/users` | Viewer+ | List users (paginated) |
| POST | `/api/admin/users` | Admin | Create user |
| PUT | `/api/admin/users/{id}` | Admin | Update user |
| DELETE | `/api/admin/users/{id}` | Admin | Delete user |

### Home
| Method | Endpoint | Auth | Description |
|--------|----------|------|-------------|
| GET | `/api/home/stats` | Viewer+ | Dashboard stats (pipeline, dashboard, domain count, domain list) |

### Lineage
| Method | Endpoint | Auth | Description |
|--------|----------|------|-------------|
| GET | `/api/lineage` | Viewer+ | Data lineage graph (validates mart tables exist) |

---

## 👥 RBAC (Role-Based Access Control)

| Role | Level | Akses |
|------|-------|-------|
| Viewer | 10 | Read-only |
| Editor | 20 | Create, edit, run |
| Admin | 30 | Full + manage users |

### Dependency Injection

```python
from app.core.security import ViewerUserDep, EditorUserDep, AdminUserDep

@router.get("/pipelines")
async def list_pipelines(current_user: ViewerUserDep):
    # Requires viewer role (level 10+)
    pass

@router.post("/pipelines")
async def create_pipeline(current_user: EditorUserDep):
    # Requires editor role (level 20+)
    pass

@router.delete("/pipelines/{id}")
async def delete_pipeline(current_user: AdminUserDep):
    # Requires admin role (level 30)
    pass
```

---

## 🗄️ Database Tables

| Table | Description |
|-------|-------------|
| `app.users` | Users dengan role |
| `app.pipelines` | Pipeline definitions |
| `app.pipeline_steps` | Steps per pipeline |
| `app.pipeline_runs` | Run history + instrumentation |
| `app.dashboards` | Dashboard definitions |
| `app.dashboard_dependencies` | Dashboard → mart table deps |
| `app.domains` | Domain grouping |
| `app.data_assets` | Registered data assets |
| `app.quality_checks` | Quality check results |
| `app.quality_rules` | Custom quality rules |
| `app.chat_sessions` | AI chat sessions |
| `app.chat_messages` | Chat messages + evidence |
| `app.activity_log` | Activity audit trail |
| `app.settings` | System settings |

---

## ⚙️ Services

| Service | Fungsi |
|---------|--------|
| `dag_generator.py` | Generate Airflow DAG Python files |
| `airflow_client.py` | Trigger/status/cancel DAG runs |
| `data_quality.py` | Run built-in + custom quality checks |
| `ai_chat.py` | Chat logic, evidence, session management |
| `gemini_client.py` | Gemini API wrapper |
| `superset_client.py` | Superset API (guest token, embed) |
| `mart_metadata.py` | Register mart tables in metadata |
| `pipeline_validator.py` | Validate step SQL & config |
| `cron_parser.py` | Convert cron → human-readable |

---

## 🧪 Data Quality Rules

### Built-in Checks
| Check | Description |
|-------|-------------|
| `not_null` | Kolom tidak boleh ada null |
| `unique` | Kolom tidak boleh ada duplikat |
| `accepted_values` | Kolom hanya boleh berisi nilai tertentu |
| `min_value` | Kolom harus >= nilai minimum |
| `max_value` | Kolom harus <= nilai maksimum |
| `row_count_min` | Row count harus >= minimum |
| `freshness_sla` | Data harus fresh dalam X jam |

---

## 🛡️ Reliability & AI Safeguards

- Chat session selalu difilter `user_id`; caller tidak dapat membaca/menghapus session user lain.
- Pipeline run memakai row lock `FOR UPDATE`; endpoint read/status/history membutuhkan viewer auth.
- Upload dataset memakai PostgreSQL `COPY`; chart identifier divalidasi sebelum dynamic SQL.
- Shared asyncpg pools ditutup saat shutdown; SQLAlchemy memakai `pool_pre_ping`, recycle, dan command timeout.
- Airflow HTTP client memakai connect/read timeout dan connection limits; Superset login memakai client terisolasi agar cookie tidak bocor antar-request.
- Request log mencatat method, path, status, latency dan response header `X-Request-Duration-Ms`.
- AI request memiliki batas panjang; structured analysis divalidasi Pydantic dengan confidence `0.0–1.0`.
- Dashboard data dibatasi, diringkas, dan dibungkus `<untrusted_dashboard_data>` agar isi chart tidak dianggap instruksi.
- Log AI hanya menyimpan model, identifier, latency, ukuran input/output, dan usage metadata—bukan prompt, chart data, API key, atau secret.

## 🧪 Tests

```bash
python -m py_compile app/models/schemas.py app/services/gemini_client.py app/services/ai_chat.py app/routers/ai.py
python -m unittest discover -s tests -v
```

Regression suite mencakup SQL single-statement, generated DAG quality gate/idempotency, bounded AI input, confidence validation, Decimal summary, empty evidence, chart-name replacement, dan prompt-injection delimiter. Test tidak memanggil Gemini network.

## 🗃️ Database Migration

Implementasi saat ini tidak menambah atau mengubah tabel, kolom, index, constraint, maupun enum PostgreSQL. Alembic migration tidak diperlukan selama schema existing sudah memiliki `app.chat_sessions.user_id` dan `app.chat_messages.evidence`.

## 🔮 Future Improvements

- [ ] Persist aggregate token/cost metrics tanpa prompt contents
- [ ] Tambah adversarial AI evaluation dan model rollout gates
- [ ] Implement WebSocket bila polling status pipeline tidak lagi mencukupi
- [ ] Tambah Redis hanya jika profiling menunjukkan cache process-local tidak cukup

---

## 🙏 Acknowledgments

- Built as part of Datapulse — Analytics Platform for Education
- Powered by FastAPI, SQLAlchemy, and Google Gemini

---

## 👤 Author

**Bagus Rahmadani**

- GitHub: [@Bagus2510](https://github.com/Bagus2510)
- LinkedIn: [bagusrahmadani](https://www.linkedin.com/in/bagusrahmadani/)
- Portfolio Website: [bagusrahmadani.vercel.app](https://bagusrahmadani.vercel.app/)
- Email: bagusrajin465@gmail.com

---

<div align="center">

**Made with ❤️ for Student Academic Success**

</div>
