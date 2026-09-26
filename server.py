"""
MCP-коннектор amoCRM -> Claude. Чтение и изменение данных (без удаления).

Все настройки берутся из переменных окружения (см. .env.example).
Токен amoCRM хранится только на сервере и никогда не попадает в ответы.
"""
from __future__ import annotations

import hashlib
import json
import os
import statistics
import time
from datetime import datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

import httpx
from mcp.server.fastmcp import FastMCP

# ---------------------------------------------------------------- настройки

SUBDOMAIN = os.environ["AMO_SUBDOMAIN"]            # например: prolider
TOKEN = os.environ["AMO_TOKEN"]                    # долгосрочный токен частной интеграции
MCP_SECRET = os.environ["MCP_SECRET"]              # длинная случайная строка, часть URL
TZ = ZoneInfo(os.getenv("TIMEZONE", "Europe/Moscow"))
CHANNEL_FIELD_ID = int(os.getenv("CHANNEL_FIELD_ID", "0") or 0)   # доп. поле «Источник», если есть
MEASURE_STATUS_IDS = {int(x) for x in os.getenv("MEASURE_STATUS_IDS", "").split(",") if x.strip()}
CONTRACT_STATUS_IDS = {int(x) for x in os.getenv("CONTRACT_STATUS_IDS", "").split(",") if x.strip()}
CALL_OK_SECONDS = int(os.getenv("CALL_OK_SECONDS", "20"))
WORK_HOURS = os.getenv("WORK_HOURS", "09:00-19:00")
WORK_DAYS = {int(x) for x in os.getenv("WORK_DAYS", "1,2,3,4,5,6").split(",")}  # 1=пн ... 7=вс
HASH_SALT = os.getenv("HASH_SALT", MCP_SECRET)
WRITE_ENABLED = os.getenv("WRITE_ENABLED", "1") == "1"   # 0 = коннектор только читает
AUDIT_LOG = os.getenv("AUDIT_LOG", "/data/audit.log")
MAX_BATCH = int(os.getenv("MAX_BATCH", "50"))

WON, LOST = 142, 143  # системные этапы amoCRM «Успешно» и «Закрыто и не реализовано»
BASE = f"https://{SUBDOMAIN}.amocrm.ru/api/v4"
PAGE = 250
MAX_OUT = 250

RESPONSE_EVENTS = ["outgoing_call", "outgoing_chat_message"]
CONTACT_EVENTS = ["incoming_chat_message"]
TIMELINE_EVENTS = [
    "lead_added", "lead_status_changed", "entity_responsible_changed",
    "incoming_call", "outgoing_call", "incoming_chat_message", "outgoing_chat_message",
]

mcp = FastMCP(
    "amoCRM",
    instructions=(
        "Доступ к amoCRM компании по заборам и воротам: чтение и изменение, без удаления. "
        "Начинай с get_reference, чтобы понять воронки и этапы. "
        "Для сводных цифр используй get_funnel_metrics, а не выгрузку всех сделок. "
        "ВСЕ изменяющие инструменты (update_leads, create_tasks, complete_task, add_notes, "
        "create_lead) сначала вызывай с confirm=false, покажи пользователю предпросмотр "
        "«было -> станет» и вызывай с confirm=true только после его явного согласия."
    ),
    host="0.0.0.0",
    port=int(os.getenv("PORT", "8000")),
    streamable_http_path=f"/mcp/{MCP_SECRET}",
    stateless_http=True,
    json_response=True,
)

# ---------------------------------------------------------------- клиент API

_client = httpx.Client(
    base_url=BASE,
    headers={"Authorization": f"Bearer {TOKEN}"},
    timeout=30,
)
_last_call = 0.0
_cache: dict[str, tuple[float, Any]] = {}


def _get(path: str, params: dict | list | None = None) -> dict:
    """GET с паузой между запросами (лимит amoCRM) и повтором при 429."""
    global _last_call
    for attempt in range(5):
        wait = 0.15 - (time.monotonic() - _last_call)
        if wait > 0:
            time.sleep(wait)
        _last_call = time.monotonic()
        r = _client.get(path, params=params)
        if r.status_code == 429:
            time.sleep(1 + attempt)
            continue
        if r.status_code == 204:
            return {}
        if r.status_code == 401:
            raise RuntimeError("amoCRM: токен недействителен или отозван (401).")
        if r.status_code == 403:
            raise RuntimeError("amoCRM: нет прав на этот раздел (403).")
        r.raise_for_status()
        return r.json()
    raise RuntimeError("amoCRM: слишком много запросов, попробуйте позже.")


def _send(method: str, path: str, body: Any) -> dict:
    """POST/PATCH с той же паузой и повтором. DELETE не используется намеренно."""
    global _last_call
    if method not in ("POST", "PATCH"):
        raise RuntimeError("Разрешены только POST и PATCH.")
    for attempt in range(5):
        wait = 0.15 - (time.monotonic() - _last_call)
        if wait > 0:
            time.sleep(wait)
        _last_call = time.monotonic()
        r = _client.request(method, path, json=body)
        if r.status_code == 429:
            time.sleep(1 + attempt)
            continue
        if r.status_code in (401, 403):
            raise RuntimeError(f"amoCRM: нет прав на изменение ({r.status_code}).")
        if r.status_code >= 400:
            raise RuntimeError(f"amoCRM отклонила изменение ({r.status_code}): {r.text[:500]}")
        return r.json() if r.content else {}
    raise RuntimeError("amoCRM: слишком много запросов, попробуйте позже.")


def _get_all(path: str, key: str, params: list[tuple[str, Any]], max_pages: int = 400) -> list[dict]:
    """Выгрузка всех страниц."""
    out: list[dict] = []
    for page in range(1, max_pages + 1):
        data = _get(path, params + [("limit", PAGE), ("page", page)])
        items = data.get("_embedded", {}).get(key, [])
        out.extend(items)
        if len(items) < PAGE or "next" not in data.get("_links", {}):
            break
    return out


def _cached(key: str, ttl: int, fn):
    now = time.time()
    if key in _cache and now - _cache[key][0] < ttl:
        return _cache[key][1]
    val = fn()
    _cache[key] = (now, val)
    return val


# ---------------------------------------------------------------- справочники

def _reference() -> dict:
    def load():
        pipelines = _get_all("/leads/pipelines", "pipelines", [])
        users = _get_all("/users", "users", [])
        fields = _get_all("/leads/custom_fields", "custom_fields", [])
        try:
            reasons = _get_all("/leads/loss_reasons", "loss_reasons", [])
        except Exception:
            reasons = []
        try:
            sources = _get_all("/sources", "sources", [])
        except Exception:
            sources = []
        statuses = {}
        for p in pipelines:
            for s in p.get("_embedded", {}).get("statuses", []):
                statuses[s["id"]] = {"name": s["name"], "pipeline_id": p["id"], "sort": s.get("sort")}
        return {
            "pipelines": {p["id"]: p["name"] for p in pipelines},
            "statuses": statuses,
            "users": {u["id"]: u["name"] for u in users},
            "fields": {f["id"]: f["name"] for f in fields},
            "loss_reasons": {r["id"]: r["name"] for r in reasons},
            "sources": {s["id"]: s["name"] for s in sources},
        }
    return _cached("ref", 3600, load)


def _ts(date_str: str, end: bool = False) -> int:
    d = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=TZ)
    if end:
        d = d + timedelta(days=1) - timedelta(seconds=1)
    return int(d.timestamp())


def _iso(ts: int | None) -> str | None:
    return datetime.fromtimestamp(ts, TZ).isoformat() if ts else None


def _channel(lead: dict) -> str:
    if CHANNEL_FIELD_ID:
        for f in lead.get("custom_fields_values") or []:
            if f.get("field_id") == CHANNEL_FIELD_ID and f.get("values"):
                return str(f["values"][0].get("value"))
    tags = lead.get("_embedded", {}).get("tags") or []
    if tags:
        return tags[0]["name"]
    for f in lead.get("custom_fields_values") or []:
        if (f.get("field_code") or "").upper() == "UTM_SOURCE" and f.get("values"):
            return str(f["values"][0].get("value"))
    return "не указан"


def _short_lead(lead: dict) -> dict:
    ref = _reference()
    st = ref["statuses"].get(lead.get("status_id"), {})
    reasons = lead.get("_embedded", {}).get("loss_reason") or []
    return {
        "id": lead["id"],
        "channel": _channel(lead),
        "pipeline": ref["pipelines"].get(lead.get("pipeline_id")),
        "status": st.get("name", lead.get("status_id")),
        "responsible": ref["users"].get(lead.get("responsible_user_id")),
        "price": lead.get("price"),
        "created_at": _iso(lead.get("created_at")),
        "closed_at": _iso(lead.get("closed_at")),
        "loss_reason": reasons[0]["name"] if reasons else None,
        "tags": [t["name"] for t in lead.get("_embedded", {}).get("tags") or []],
    }


def _leads(date_from: str, date_to: str, pipeline_id: int | None = None,
           responsible_user_id: int | None = None) -> list[dict]:
    params: list[tuple[str, Any]] = [
        ("filter[created_at][from]", _ts(date_from)),
        ("filter[created_at][to]", _ts(date_to, end=True)),
        ("with", "loss_reason,contacts"),
    ]
    if pipeline_id:
        params.append(("filter[pipeline_id][]", pipeline_id))
    if responsible_user_id:
        params.append(("filter[responsible_user_id][]", responsible_user_id))
    key = f"leads:{params}"
    return _cached(key, 600, lambda: _get_all("/leads", "leads", params))


def _events(date_from_ts: int, date_to_ts: int, types: list[str]) -> list[dict]:
    params: list[tuple[str, Any]] = [
        ("filter[entity]", "lead"),
        ("filter[created_at][from]", date_from_ts),
        ("filter[created_at][to]", date_to_ts),
    ] + [("filter[type][]", t) for t in types]
    key = f"events:{params}"
    return _cached(key, 600, lambda: _get_all("/events", "events", params, max_pages=2000))


def _call_notes(date_from_ts: int, date_to_ts: int, entity: str = "leads") -> list[dict]:
    """Звонки-примечания. Телефония может писать их в сделку (leads) или в контакт (contacts)."""
    params: list[tuple[str, Any]] = [
        ("filter[note_type][]", "call_in"),
        ("filter[note_type][]", "call_out"),
        ("filter[updated_at][from]", date_from_ts),
        ("filter[updated_at][to]", date_to_ts),
    ]
    key = f"notes:{entity}:{params}"
    return _cached(key, 600, lambda: _get_all(f"/{entity}/notes", "notes", params, max_pages=1000))


def _lead_contacts(lead: dict) -> list[int]:
    return [c["id"] for c in lead.get("_embedded", {}).get("contacts") or []]


def _is_work_time(ts: int) -> bool:
    d = datetime.fromtimestamp(ts, TZ)
    start, end = WORK_HOURS.split("-")
    return d.isoweekday() in WORK_DAYS and start <= d.strftime("%H:%M") < end


def _stats(values: list[float]) -> dict:
    if not values:
        return {"n": 0, "median_min": None, "p90_min": None}
    v = sorted(values)
    p90 = v[min(len(v) - 1, int(round(0.9 * (len(v) - 1))))]
    return {"n": len(v), "median_min": round(statistics.median(v), 1), "p90_min": round(p90, 1)}


# ---------------------------------------------------------------- инструменты

@mcp.tool()
def get_reference() -> dict:
    """Справочники amoCRM: воронки, этапы (с id), менеджеры, доп. поля сделок,
    причины отказа, источники. Вызывай первым, чтобы понимать структуру CRM."""
    ref = _reference()
    return {
        "pipelines": [{"id": k, "name": v} for k, v in ref["pipelines"].items()],
        "statuses": [{"id": k, **v} for k, v in ref["statuses"].items()],
        "users": [{"id": k, "name": v} for k, v in ref["users"].items()],
        "lead_fields": [{"id": k, "name": v} for k, v in ref["fields"].items()],
        "loss_reasons": list(ref["loss_reasons"].values()),
        "sources": list(ref["sources"].values()),
        "settings": {
            "channel_field_id": CHANNEL_FIELD_ID or None,
            "measure_status_ids": sorted(MEASURE_STATUS_IDS),
            "contract_status_ids": sorted(CONTRACT_STATUS_IDS),
            "call_ok_seconds": CALL_OK_SECONDS,
            "work_hours": WORK_HOURS, "work_days": sorted(WORK_DAYS),
        },
    }


@mcp.tool()
def search_leads(date_from: str, date_to: str, pipeline_id: int | None = None,
                 responsible_user_id: int | None = None, status_id: int | None = None,
                 channel: str | None = None, only_open: bool = False,
                 offset: int = 0) -> dict:
    """Сделки, созданные в период (даты YYYY-MM-DD). Фильтры: воронка, менеджер,
    этап, канал, только открытые. Возвращает до 250 записей за вызов; для следующих
    передай offset. Имена и контакты клиентов не возвращаются."""
    leads = _leads(date_from, date_to, pipeline_id, responsible_user_id)
    rows = []
    for ld in leads:
        if status_id and ld.get("status_id") != status_id:
            continue
        if only_open and ld.get("status_id") in (WON, LOST):
            continue
        s = _short_lead(ld)
        if channel and s["channel"].lower() != channel.lower():
            continue
        rows.append(s)
    return {"total": len(rows), "offset": offset, "items": rows[offset:offset + MAX_OUT]}


@mcp.tool()
def get_lead(lead_id: int) -> dict:
    """Одна сделка: основные данные, хронология событий, задачи и звонки."""
    lead = _get(f"/leads/{lead_id}", {"with": "loss_reason,contacts"})
    events = _get_all("/events", "events", [
        ("filter[entity]", "lead"), ("filter[entity_id][]", lead_id),
    ] + [("filter[type][]", t) for t in TIMELINE_EVENTS])
    tasks = _get_all("/tasks", "tasks", [
        ("filter[entity_type]", "leads"), ("filter[entity_id][]", lead_id),
    ])
    call_filter = [("filter[note_type][]", "call_in"), ("filter[note_type][]", "call_out")]
    notes = _get_all(f"/leads/{lead_id}/notes", "notes", call_filter)
    for cid in _lead_contacts(lead):
        notes += [n for n in _get_all(f"/contacts/{cid}/notes", "notes", call_filter)
                  if (n.get("created_at") or 0) >= lead.get("created_at", 0)]
    ref = _reference()
    return {
        "lead": _short_lead(lead),
        "timeline": sorted(({
            "at": _iso(e["created_at"]), "type": e["type"],
            "by": ref["users"].get(e.get("created_by")),
        } for e in events), key=lambda x: x["at"]),
        "tasks": [{
            "text": (t.get("text") or "")[:200],
            "due": _iso(t.get("complete_till")),
            "done": t.get("is_completed"),
            "responsible": ref["users"].get(t.get("responsible_user_id")),
            "result": ((t.get("result") or {}).get("text") or "")[:200],
        } for t in tasks],
        "calls": [{
            "at": _iso(n.get("created_at")),
            "direction": "входящий" if n.get("note_type") == "call_in" else "исходящий",
            "duration_sec": (n.get("params") or {}).get("duration"),
            "by": ref["users"].get(n.get("created_by")),
        } for n in notes],
    }


@mcp.tool()
def get_events(date_from: str, date_to: str, types: list[str] | None = None,
               offset: int = 0) -> dict:
    """Поток событий по сделкам за период. Типы: lead_added, lead_status_changed,
    entity_responsible_changed, incoming_call, outgoing_call, incoming_chat_message,
    outgoing_chat_message. По умолчанию все эти типы. До 250 записей за вызов."""
    ev = _events(_ts(date_from), _ts(date_to, end=True), types or TIMELINE_EVENTS)
    ref = _reference()
    rows = [{
        "lead_id": e["entity_id"], "at": _iso(e["created_at"]), "type": e["type"],
        "by": ref["users"].get(e.get("created_by")),
        "status_after": ref["statuses"].get(
            ((e.get("value_after") or [{}])[0].get("lead_status") or {}).get("id"), {}).get("name")
        if e["type"] == "lead_status_changed" else None,
    } for e in ev]
    return {"total": len(rows), "offset": offset, "items": rows[offset:offset + MAX_OUT]}


@mcp.tool()
def get_tasks(only_open: bool = True, only_overdue: bool = False,
              responsible_user_id: int | None = None, offset: int = 0) -> dict:
    """Задачи по сделкам: открытые и/или просроченные, по менеджеру."""
    params: list[tuple[str, Any]] = [("filter[entity_type]", "leads")]
    if only_open:
        params.append(("filter[is_completed]", 0))
    if responsible_user_id:
        params.append(("filter[responsible_user_id][]", responsible_user_id))
    tasks = _get_all("/tasks", "tasks", params)
    now = int(time.time())
    if only_overdue:
        tasks = [t for t in tasks if not t.get("is_completed") and (t.get("complete_till") or now) < now]
    ref = _reference()
    rows = [{
        "lead_id": t.get("entity_id"), "text": (t.get("text") or "")[:200],
        "due": _iso(t.get("complete_till")), "done": t.get("is_completed"),
        "responsible": ref["users"].get(t.get("responsible_user_id")),
    } for t in tasks]
    return {"total": len(rows), "offset": offset, "items": rows[offset:offset + MAX_OUT]}


@mcp.tool()
def get_unsorted(offset: int = 0) -> dict:
    """Заявки, которые сейчас лежат в «Неразобранном» и ещё не приняты менеджером."""
    items = _get_all("/leads/unsorted", "unsorted", [])
    now = int(time.time())
    rows = [{
        "uid": u.get("uid"), "category": u.get("category"),
        "source": u.get("source_name") or u.get("source_uid"),
        "created_at": _iso(u.get("created_at")),
        "waiting_min": round((now - (u.get("created_at") or now)) / 60),
    } for u in items]
    return {"total": len(rows), "offset": offset, "items": rows[offset:offset + MAX_OUT]}


@mcp.tool()
def get_funnel_metrics(date_from: str, date_to: str, group_by: str = "channel") -> dict:
    """Главные показатели воронки за период (даты YYYY-MM-DD), по сделкам, созданным
    в этот период. group_by: channel | manager | month.
    Возвращает: число заявок; время до первой попытки ответа и до первого
    состоявшегося контакта (медиана и 90-й перцентиль, минуты) отдельно для рабочего
    и нерабочего времени; долю заявок без ответа; число исходящих звонков до контакта;
    конверсию в замер и договор; причины отказов."""
    leads = _leads(date_from, date_to)
    if not leads:
        return {"groups": {}, "note": "За период сделок нет."}
    t0 = min(ld["created_at"] for ld in leads)
    t1 = int(time.time())
    events = _events(t0, t1, ["lead_status_changed"] + RESPONSE_EVENTS + CONTACT_EVENTS)
    ref = _reference()

    by_lead: dict[int, list[dict]] = {}
    for e in events:
        by_lead.setdefault(e["entity_id"], []).append(e)
    calls_by_lead: dict[int, list[dict]] = {}
    for n in _call_notes(t0, t1, "leads"):
        calls_by_lead.setdefault(n.get("entity_id"), []).append(n)
    # Звонки, записанные в контакт, засчитываем всем сделкам этого контакта
    # (фильтр «после создания сделки» ниже отсекает звонки по старым сделкам).
    leads_by_contact: dict[int, list[int]] = {}
    for ld in leads:
        for cid in _lead_contacts(ld):
            leads_by_contact.setdefault(cid, []).append(ld["id"])
    for n in _call_notes(t0, t1, "contacts"):
        for lid in leads_by_contact.get(n.get("entity_id"), []):
            calls_by_lead.setdefault(lid, []).append(n)

    groups: dict[str, dict] = {}
    for ld in leads:
        if group_by == "manager":
            g = ref["users"].get(ld.get("responsible_user_id"), "без ответственного")
        elif group_by == "month":
            g = datetime.fromtimestamp(ld["created_at"], TZ).strftime("%Y-%m")
        else:
            g = _channel(ld)
        G = groups.setdefault(g, {
            "leads": 0, "first_try_work": [], "first_try_off": [],
            "first_contact_work": [], "first_contact_off": [], "no_response": 0,
            "attempts": [], "measure": 0, "contract": 0, "lost": 0, "reasons": {},
        })
        G["leads"] += 1
        created = ld["created_at"]
        work = _is_work_time(created)
        evs = sorted(by_lead.get(ld["id"], []), key=lambda e: e["created_at"])
        calls = sorted(calls_by_lead.get(ld["id"], []), key=lambda n: n.get("created_at") or 0)

        tries = [e["created_at"] for e in evs if e["type"] in RESPONSE_EVENTS and e["created_at"] >= created]
        tries += [n["created_at"] for n in calls if n.get("note_type") == "call_out" and n.get("created_at", 0) >= created]
        if tries:
            G["first_try_work" if work else "first_try_off"].append((min(tries) - created) / 60)
        else:
            G["no_response"] += 1

        contacts = [e["created_at"] for e in evs if e["type"] in CONTACT_EVENTS and e["created_at"] >= created]
        ok_calls = [n for n in calls if ((n.get("params") or {}).get("duration") or 0) >= CALL_OK_SECONDS
                    and n.get("created_at", 0) >= created]
        contacts += [n["created_at"] for n in ok_calls]
        if contacts:
            first_contact = min(contacts)
            G["first_contact_work" if work else "first_contact_off"].append((first_contact - created) / 60)
            G["attempts"].append(sum(1 for n in calls if n.get("note_type") == "call_out"
                                     and created <= n.get("created_at", 0) <= first_contact))

        reached = {ld.get("status_id")}
        for e in evs:
            if e["type"] == "lead_status_changed":
                st = ((e.get("value_after") or [{}])[0].get("lead_status") or {}).get("id")
                if st:
                    reached.add(st)
        contract = bool(reached & CONTRACT_STATUS_IDS) or ld.get("status_id") == WON
        if contract or reached & MEASURE_STATUS_IDS:
            G["measure"] += 1
        if contract:
            G["contract"] += 1
        if ld.get("status_id") == LOST:
            G["lost"] += 1
            rs = ld.get("_embedded", {}).get("loss_reason") or []
            name = rs[0]["name"] if rs else "не указана"
            G["reasons"][name] = G["reasons"].get(name, 0) + 1

    out = {}
    for g, G in sorted(groups.items(), key=lambda kv: -kv[1]["leads"]):
        n = G["leads"]
        out[g] = {
            "leads": n,
            "first_try_work_hours": _stats(G["first_try_work"]),
            "first_try_off_hours": _stats(G["first_try_off"]),
            "first_contact_work_hours": _stats(G["first_contact_work"]),
            "first_contact_off_hours": _stats(G["first_contact_off"]),
            "no_response_share": round(G["no_response"] / n, 3),
            "median_calls_to_contact": statistics.median(G["attempts"]) if G["attempts"] else None,
            "to_measure_share": round(G["measure"] / n, 3) if MEASURE_STATUS_IDS else "не заданы MEASURE_STATUS_IDS",
            "to_contract_share": round(G["contract"] / n, 3),
            "lost": G["lost"],
            "loss_reasons": dict(sorted(G["reasons"].items(), key=lambda kv: -kv[1])),
        }
    return {
        "period": [date_from, date_to], "group_by": group_by, "groups": out,
        "notes": [
            "Попытка = первый исходящий звонок или сообщение после создания сделки.",
            f"Контакт = звонок от {CALL_OK_SECONDS} с или входящее сообщение клиента.",
            "Звонки видны, только если телефония пишет их в amoCRM (в сделку или в контакт).",
            "Звонок контакта засчитывается всем его сделкам, созданным до звонка.",
        ],
    }


@mcp.tool()
def find_stalled_leads(days_without_activity: int = 3, status_id: int | None = None,
                       offset: int = 0) -> dict:
    """Открытые сделки без открытой задачи или без изменений дольше N дней.
    Это кандидаты на дожим, которые сейчас никто не ведёт."""
    now = int(time.time())
    params: list[tuple[str, Any]] = [("filter[updated_at][to]", now - days_without_activity * 86400)]
    if status_id:
        params.append(("filter[statuses][0][status_id]", status_id))
        params.append(("filter[statuses][0][pipeline_id]",
                       _reference()["statuses"].get(status_id, {}).get("pipeline_id")))
    leads = [ld for ld in _get_all("/leads", "leads", params) if ld.get("status_id") not in (WON, LOST)]
    no_task = [ld for ld in leads if not ld.get("closest_task_at")]
    rows = []
    for ld in leads:
        s = _short_lead(ld)
        s["days_since_update"] = round((now - ld.get("updated_at", now)) / 86400, 1)
        s["has_open_task"] = bool(ld.get("closest_task_at"))
        rows.append(s)
    rows.sort(key=lambda r: (r["has_open_task"], -r["days_since_update"]))
    return {"total": len(rows), "without_task": len(no_task), "offset": offset,
            "items": rows[offset:offset + MAX_OUT]}


@mcp.tool()
def phone_hash(lead_id: int) -> dict:
    """Хеши телефонов контактов сделки (без самих номеров) — чтобы находить
    повторных клиентов и дубли."""
    lead = _get(f"/leads/{lead_id}", {"with": "contacts"})
    hashes = []
    for c in lead.get("_embedded", {}).get("contacts") or []:
        contact = _get(f"/contacts/{c['id']}")
        for f in contact.get("custom_fields_values") or []:
            if f.get("field_code") == "PHONE":
                for v in f.get("values") or []:
                    digits = "".join(ch for ch in str(v.get("value")) if ch.isdigit())[-10:]
                    if digits:
                        hashes.append(hashlib.sha256((HASH_SALT + digits).encode()).hexdigest()[:16])
    return {"lead_id": lead_id, "phone_hashes": sorted(set(hashes))}



# ---------------------------------------------------------------- изменение данных

def _audit(action: str, payload: Any, result: Any) -> None:
    try:
        os.makedirs(os.path.dirname(AUDIT_LOG) or ".", exist_ok=True)
        with open(AUDIT_LOG, "a", encoding="utf-8") as f:
            f.write(json.dumps({
                "at": datetime.now(TZ).isoformat(), "action": action,
                "payload": payload, "result": result,
            }, ensure_ascii=False, default=str) + "\n")
    except Exception:
        pass  # журнал не должен ронять изменение


def _check_write(items: list, what: str) -> None:
    if not WRITE_ENABLED:
        raise RuntimeError("Изменение данных выключено (WRITE_ENABLED=0).")
    if not items:
        raise RuntimeError(f"Пустой список: {what}.")
    if len(items) > MAX_BATCH:
        raise RuntimeError(f"Не больше {MAX_BATCH} объектов за вызов, передано {len(items)}. Разбейте на части.")


def _parse_due(value: str) -> int:
    for fmt in ("%Y-%m-%d %H:%M", "%Y-%m-%dT%H:%M", "%Y-%m-%d"):
        try:
            return int(datetime.strptime(value, fmt).replace(tzinfo=TZ).timestamp())
        except ValueError:
            continue
    raise RuntimeError(f"Не понял дату «{value}». Формат: ГГГГ-ММ-ДД ЧЧ:ММ.")


def _field_values(custom_fields: dict | None) -> list[dict]:
    out = []
    for fid, val in (custom_fields or {}).items():
        vals = val if isinstance(val, list) else [val]
        out.append({"field_id": int(fid), "values": [v if isinstance(v, dict) else {"value": v} for v in vals]})
    return out


@mcp.tool()
def update_leads(changes: list[dict], confirm: bool = False) -> dict:
    """ИЗМЕНЯЕТ сделки (до 50 за вызов). Каждый элемент changes:
    {"lead_id": 123, "status_id": ..., "pipeline_id": ..., "responsible_user_id": ...,
     "price": ..., "loss_reason_id": ..., "tags_add": ["дожим"],
     "custom_fields": {"<field_id>": "значение"}} — указывай только то, что меняешь.
    Закрыть как отказ: status_id=143 и loss_reason_id. confirm=false — только
    предпросмотр «было -> станет»; confirm=true — применить. Удаления нет."""
    _check_write(changes, "изменения сделок")
    ref = _reference()
    preview, body = [], []
    for ch in changes:
        lid = int(ch["lead_id"])
        cur = _get(f"/leads/{lid}")
        item: dict[str, Any] = {"id": lid}
        diff = {}
        for k in ("status_id", "pipeline_id", "responsible_user_id", "price", "loss_reason_id"):
            if k in ch and ch[k] is not None:
                item[k] = int(ch[k])
                before, after = cur.get(k), int(ch[k])
                if k == "status_id":
                    before = ref["statuses"].get(before, {}).get("name", before)
                    after = ref["statuses"].get(after, {}).get("name", after)
                if k == "responsible_user_id":
                    before, after = ref["users"].get(before, before), ref["users"].get(after, after)
                if k == "loss_reason_id":
                    before, after = None, ref["loss_reasons"].get(after, after)
                diff[k] = {"было": before, "станет": after}
        if ch.get("tags_add"):
            item["tags_to_add"] = [{"name": t} for t in ch["tags_add"]]
            diff["теги"] = {"добавить": ch["tags_add"]}
        if ch.get("custom_fields"):
            item["custom_fields_values"] = _field_values(ch["custom_fields"])
            curf = {f["field_id"]: [v.get("value") for v in f.get("values", [])]
                    for f in cur.get("custom_fields_values") or []}
            for fid, val in ch["custom_fields"].items():
                diff[ref["fields"].get(int(fid), fid)] = {"было": curf.get(int(fid)), "станет": val}
        preview.append({"lead_id": lid, "изменения": diff})
        body.append(item)
    if not confirm:
        return {"режим": "предпросмотр, ничего не изменено", "сделок": len(body), "preview": preview}
    res = _send("PATCH", "/leads", body)
    _audit("update_leads", body, res)
    _cache.clear()
    return {"режим": "применено", "сделок": len(body), "preview": preview}


@mcp.tool()
def create_tasks(tasks: list[dict], confirm: bool = False) -> dict:
    """СОЗДАЁТ задачи по сделкам (до 50 за вызов). Элемент:
    {"lead_id": 123, "text": "Перезвонить по откатным воротам",
     "due": "2026-09-28 11:00", "responsible_user_id": 7 (необязательно — ответственный сделки),
     "task_type_id": 1 (1 = звонок, 2 = встреча)}. confirm=false — предпросмотр."""
    _check_write(tasks, "задачи")
    ref = _reference()
    body, preview = [], []
    for t in tasks:
        lid = int(t["lead_id"])
        resp = t.get("responsible_user_id") or _get(f"/leads/{lid}").get("responsible_user_id")
        due = _parse_due(t["due"])
        body.append({
            "entity_id": lid, "entity_type": "leads", "text": t["text"],
            "complete_till": due, "responsible_user_id": int(resp),
            "task_type_id": int(t.get("task_type_id", 1)),
        })
        preview.append({"lead_id": lid, "text": t["text"], "due": _iso(due),
                        "responsible": ref["users"].get(int(resp), resp)})
    if not confirm:
        return {"режим": "предпросмотр, ничего не создано", "задач": len(body), "preview": preview}
    res = _send("POST", "/tasks", body)
    _audit("create_tasks", body, res)
    return {"режим": "создано", "задач": len(body), "preview": preview}


@mcp.tool()
def complete_task(task_id: int, result_text: str, confirm: bool = False) -> dict:
    """ЗАКРЫВАЕТ задачу с результатом. confirm=false — предпросмотр."""
    _check_write([task_id], "задача")
    cur = _get(f"/tasks/{task_id}")
    preview = {"task_id": task_id, "text": (cur.get("text") or "")[:200], "result": result_text}
    if not confirm:
        return {"режим": "предпросмотр, ничего не изменено", "preview": preview}
    body = [{"id": task_id, "is_completed": True, "result": {"text": result_text}}]
    res = _send("PATCH", "/tasks", body)
    _audit("complete_task", body, res)
    return {"режим": "применено", "preview": preview}


@mcp.tool()
def add_notes(notes: list[dict], confirm: bool = False) -> dict:
    """ДОБАВЛЯЕТ текстовые примечания в сделки (до 50 за вызов).
    Элемент: {"lead_id": 123, "text": "..."}. confirm=false — предпросмотр."""
    _check_write(notes, "примечания")
    body = [{"entity_id": int(n["lead_id"]), "note_type": "common",
             "params": {"text": n["text"]}} for n in notes]
    if not confirm:
        return {"режим": "предпросмотр, ничего не добавлено", "примечаний": len(body), "preview": notes}
    res = _send("POST", "/leads/notes", body)
    _audit("add_notes", body, res)
    return {"режим": "добавлено", "примечаний": len(body)}


@mcp.tool()
def create_lead(name: str, pipeline_id: int, status_id: int, responsible_user_id: int,
                price: int | None = None, tags: list[str] | None = None,
                custom_fields: dict | None = None, confirm: bool = False) -> dict:
    """СОЗДАЁТ сделку — например, для заказов, прошедших мимо отдела продаж
    (знакомые, соседи, повторные), чтобы они попали в воронку. confirm=false — предпросмотр."""
    _check_write([name], "сделка")
    ref = _reference()
    item: dict[str, Any] = {"name": name, "pipeline_id": pipeline_id, "status_id": status_id,
                            "responsible_user_id": responsible_user_id}
    if price is not None:
        item["price"] = price
    if tags:
        item["_embedded"] = {"tags": [{"name": t} for t in tags]}
    if custom_fields:
        item["custom_fields_values"] = _field_values(custom_fields)
    preview = {"name": name, "pipeline": ref["pipelines"].get(pipeline_id, pipeline_id),
               "status": ref["statuses"].get(status_id, {}).get("name", status_id),
               "responsible": ref["users"].get(responsible_user_id, responsible_user_id),
               "price": price, "tags": tags, "custom_fields": custom_fields}
    if not confirm:
        return {"режим": "предпросмотр, ничего не создано", "preview": preview}
    res = _send("POST", "/leads", [item])
    _audit("create_lead", item, res)
    _cache.clear()
    new_id = (res.get("_embedded", {}).get("leads") or [{}])[0].get("id")
    return {"режим": "создано", "lead_id": new_id, "preview": preview}


@mcp.tool()
def get_audit_log(last: int = 50) -> dict:
    """Последние изменения, сделанные через коннектор: что, когда, в каких объектах."""
    try:
        with open(AUDIT_LOG, encoding="utf-8") as f:
            lines = f.readlines()[-last:]
    except FileNotFoundError:
        return {"items": [], "note": "Изменений через коннектор ещё не было."}
    items = []
    for ln in lines:
        d = json.loads(ln)
        items.append({"at": d["at"], "action": d["action"],
                      "payload": json.dumps(d["payload"], ensure_ascii=False)[:500]})
    return {"items": items}


if __name__ == "__main__":
    mcp.run(transport="streamable-http")
