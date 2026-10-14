"""Local webhook demonstration; run only through the optional Compose demo profile."""

from collections import defaultdict, deque

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import JSONResponse

app = FastAPI(title="Local webhook receiver", docs_url=None, redoc_url=None)
received: deque[dict[str, object]] = deque(maxlen=1000)
attempts: defaultdict[str, int] = defaultdict(int)


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/events")
async def receive_event(
    request: Request,
    fail_first: int = Query(default=0, ge=0, le=100),
) -> JSONResponse:
    payload: object = await request.json()
    if not isinstance(payload, dict):
        raise HTTPException(status_code=422, detail="Expected a JSON object")
    event_id = str(payload.get("event_id", payload.get("payment_id", "unknown")))
    attempts[event_id] += 1
    attempt = attempts[event_id]
    status_code = 503 if attempt <= fail_first else 200
    received.append(
        {
            "event_id": event_id,
            "attempt": attempt,
            "status_code": status_code,
            "payload": payload,
        }
    )
    return JSONResponse({"accepted": status_code == 200}, status_code=status_code)


@app.get("/events")
async def list_events() -> dict[str, object]:
    return {"events": list(received)}
