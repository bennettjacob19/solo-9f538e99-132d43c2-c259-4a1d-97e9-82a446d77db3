# PostgreSQL 演练台（行锁 + 发布两阶段提交）

本服务包含两个相互独立的演练页：

1. **行锁演练台**（`/`）：当一个 `UPDATE` 被 PostgreSQL 行锁挂住时，从网页上找出**真实**的
   持锁者，并只解除本演练造成的这一次阻塞。
2. **发布两阶段提交演练台**（`/release`）：登记发布单（登记库）的同时在另一个独立的
   PostgreSQL 数据库扣减发布额度（额度库），用真实的
   `PREPARE TRANSACTION / COMMIT PREPARED / ROLLBACK PREPARED` 演练跨库两阶段提交（2PC）。

---

## 发布两阶段提交演练（/release）

### 业务规则与一致性保证

- **两个独立的 PostgreSQL 数据库**
  - 登记库：`release_orders`（已登记发布单）、`tx_decisions`（持久化提交/撤销决定）、
    `order_intents`（预备中的在途版本，独立于 XA 事务）。
  - 额度库：`release_quota`（单行额度账户）、`release_quota_ledger`（按单号记账的扣减台账）。
- **真实两阶段提交**：两库使用同一个由单号确定性生成的 GID（`xa_rel_<单号>`），
  依次 `PREPARE TRANSACTION`，再 `COMMIT PREPARED`。
- **额度不足两边都不生效**：额度库预备阶段发现 `remaining < 1` 时回滚自身事务，
  登记库的预备事务随之 `ROLLBACK PREPARED`，两库均无可见变更。
- **持久化提交决定**：两库都预备成功后、提交任何一方之前，先在登记库以独立自动提交事务
  写入 `tx_decisions(decision=commit)`。重启 Web 后：
  - **没有持久化提交决定**的预备事务一律撤销（`ROLLBACK PREPARED`），页面显示“已撤销”；
  - **有提交决定**则把尚未完成的参与者补提交（`COMMIT PREPARED`）。
- **页面状态全部按库中实况读取**：登记库/额度库各自显示“未生效 / 已预备 / 已提交”，
  依据是 `pg_prepared_xacts`、`release_orders` 与 `release_quota_ledger` 的真实内容，
  并实时显示剩余额度。仅当两库都已提交才显示“发布成功”。
- **幂等与冲突**：重复提交或继续处理沿用原单，每单至多登记一次、扣减一次；
  同一单号再次提交**不同版本**返回 409 冲突。被撤销的单（从未登记、未扣减）允许复用单号重提。
- **两个可暂停的演练点**
  - 暂停点 A：登记库已 PREPARE、额度库尚未预备（无提交决定，重启后继续会被撤销）；
  - 暂停点 B：登记库已 COMMIT PREPARED、额度库仍 PREPARED（已有提交决定，重启后继续会补提交）。
- **重启安全**：启动建表/种子使用无锁 MVCC 检查并设置 `lock_timeout`，
  不会被额度库预备事务持有的行锁阻塞；重启后按原单号打开页面，点“继续处理”即按上述规则恢复。

### 运行（Docker Compose）

```bash
docker compose up --build
# 行锁演练台  http://localhost:8000/
# 发布 2PC    http://localhost:8000/release
```

Compose 启动三个 PostgreSQL 16 容器（行锁库 `db`、登记库 `registry-db`、
额度库 `quota-db`，后两者均以 `-c max_prepared_transactions=20` 开启两阶段提交）
和单 worker 的 Web 服务。

### 本地运行

```bash
python -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
export DATABASE_URL=postgresql://drill:drill@localhost:5432/drill
export REGISTRY_DATABASE_URL=postgresql://registry_app:registry_app@localhost:5433/registry
export QUOTA_DATABASE_URL=postgresql://quota_app:quota_app@localhost:5434/quota
uvicorn app.main:app --port 8000
```

> 两个参与库都必须把 `max_prepared_transactions` 设为大于 0（例如 20），
> 否则 `PREPARE TRANSACTION` 会被拒绝。

### 配置（发布演练）

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `REGISTRY_DATABASE_URL` | （同 `DATABASE_URL`） | 登记库连接串 |
| `QUOTA_DATABASE_URL` | （同 `DATABASE_URL`） | 额度库连接串 |
| `INITIAL_QUOTA` | `5` | 全新额度库种子额度 |
| `RELEASE_APP_NAME` | `releasedrill` | 连接 `application_name` 前缀 |
| `RELEASE_DB_CONNECT_TIMEOUT` | `5` | 连接超时（秒） |

### 测试（发布演练）

需要两个独立的真实 PostgreSQL 数据库（均开启 `max_prepared_transactions`）：

```bash
TEST_REGISTRY_URL=postgresql://u@localhost/registry_test \
TEST_QUOTA_URL=postgresql://u@localhost/quota_test \
pytest tests/test_release_2pc.py -q
```

覆盖：两库正常提交、同单同版本幂等、同单异版本 409 冲突、暂停点 A 进程内继续、
暂停点 A 重启后无决定撤销、暂停点 B 重启后按决定补提交、启动不被预备事务行锁阻塞、
额度不足两边均不生效、撤销单号可复用、未知单号被拒绝。

### 接口（发布演练）

- 页面：`GET /release`、`GET /release/orders/{单号}`
- 表单：`POST /release/submit`、`POST /release/orders/{单号}/continue`
- JSON：`GET /release/api/orders`、`GET /release/api/orders/{单号}`、
  `POST /release/api/submit`、`POST /release/api/orders/{单号}/continue`

---

## 行锁演练台（/）


## 功能

- **真实数据库会话**：每轮演练在 PostgreSQL 中创建两个连接
  - 持锁会话：`BEGIN` 后执行 `SELECT ... FOR UPDATE` 锁住演练行（`idle in transaction`）；
  - 受阻更新：另一连接执行 `UPDATE lock_drill_row …`，被行锁挂住。
- **页面展示**：两端 PID、`backend_start`（会话启动时间）、事务开始时间、当前状态、
  `wait_event_type/wait_event`、`pg_blocking_pids()` 给出的**实际阻塞者**、
  双方持有的锁及阻塞关系明细表。
- **安全解除**
  - 只在「当前演练状态为 blocked，且数据库刚确认 `holder_pid ∈ pg_blocking_pids(waiter_pid)`」
    时开放按钮；
  - 提交时用**单条 SQL**（`_TERMINATE_IF_STILL_BLOCKING_SQL`）原子地复核
    「原会话仍存活（pid + backend_start + 记录中的 application_name）且仍阻塞原更新」，
    复核通过才 `pg_terminate_backend`，因此 pid 复用、会话改名、阻塞关系变化都不会误杀。
  - 页面令牌与轮次一一对应：旧页面（会话结束、服务重启、开始新一轮后）提交会被
    403/410 拒绝，绝不会关闭新会话。
- **以真实结果判定**：解锁后等待受阻连接真实执行 `COMMIT`；判定只看
  `UPDATE ... RETURNING` + `COMMIT` 的结果，并用独立连接复核行值。
  - 终止失败、命令返回成功但 `COMMIT` 未完成、或仍存在其他阻塞者时，判定为
    **未恢复**，并列出当前阻塞者。
  - 重复提交返回同一判定，不会再次终止任何会话。
- **清理**：`结束本轮演练`、开始新一轮、服务关闭都会按身份三元组终止本演练的两个会话；
  启动时清理上一进程遗留的 `lockdrill:drill:*` 会话。

## 运行（Docker Compose）

```bash
docker compose up --build
# 打开 http://localhost:8000
```

`docker compose` 会启动 PostgreSQL 16 与 Web 应用。Web 服务固定单 worker，
因为演练会话与页面令牌保存在进程内（会话本身在 PostgreSQL 中）。

## 本地运行

```bash
python -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
export DATABASE_URL=postgresql://drill:drill@localhost:5432/drill
uvicorn app.main:app --port 8000
```

## 配置

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `DATABASE_URL` | `postgresql://drill:drill@localhost:5432/drill` | 应用连接串 |
| `DRILL_SESSION_PREFIX` | `lockdrill` | 会话 `application_name` 前缀 |
| `BLOCK_CONFIRM_SECONDS` | `5` | 等待数据库确认阻塞关系的时间 |
| `RELEASE_WAIT_SECONDS` | `10` | 终止后等待受阻更新真实结束的时间 |
| `REFUSED_WAIT_SECONDS` | `2` | 未终止时等待其自行结束的时间 |
| `DRILL_TTL_SECONDS` | `1800` | 超时自动结束的演练时长 |
| `DB_CONNECT_TIMEOUT` | `5` | 连接超时（秒） |

> 注意：生产库上使用 `pg_terminate_backend` 需要相应权限；演练请使用专用数据库/角色。

## 测试

测试需要真实 PostgreSQL（会创建 `lock_drill_test` 角色与数据库，请指向一次性实例）：

```bash
TEST_DATABASE_URL=postgresql://postgres@localhost/postgres pytest -q
```

覆盖：真实阻塞关系、正常解锁恢复、服务重启后的旧页面、结束演练后的旧页面、
会话身份被篡改（拒绝终止且不释放锁）、终止成功但无提交结果（判未恢复）、
旧会话已消失但更新已提交（按真实结果判恢复）、重复解锁幂等。

## 接口

- `GET /`、`POST /drill/start`、`GET /drill/{id}`
- `POST /drill/{id}/release`、`POST /drill/{id}/end`（表单，含 `token`）
- `GET /api/state`、`GET /api/drills/{id}`、`POST /api/drills/{id}/release|end`（JSON）
- `GET /healthz`
