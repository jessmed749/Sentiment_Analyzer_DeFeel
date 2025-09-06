import os
from fastapi import FastAPI, Body, HTTPException
from pydantic import BaseModel, Field, ConfigDict
import time, json, logging
from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST
from fastapi.responses import PlainTextResponse
from transformers import pipeline
from dotenv import load_dotenv



# Metrics setup
REQS = Counter("sentiment_inference_requests_total", "HTTP requests", ["endpoint","method","status"])
INFER_MS = Histogram("sentiment_inference_inference_latency_ms", "Model inference latency (ms)")
REQ_MS = Histogram("sentiment_inference_request_latency_ms", "Request latency (ms)")
SCORED = Counter("sentiment_inference_scored_total", "Scored messages total")


app = FastAPI()

# Load environment variables
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '../../.env.dev'))

MODEL_PATH = os.getenv("MODEL_PATH", "distilbert-base-uncased-finetuned-sst-2-english").strip('" ')
APP_ENV = os.getenv("APP_ENV", "dev").strip('" ')

logging.basicConfig(level=logging.INFO)



class PredictRequest(BaseModel):
    id: str = Field(..., example="42")
    source: str = Field(..., example="curl")
    text: str = Field(..., example="I love this jeans so much")
    lang: str = Field(..., example="en")
    created_at: str = Field(..., example="2024-01-01T00:00:00Z")
    meta: dict = Field(default_factory=dict, example={"topic": "fashion"})

    model_config = ConfigDict(extra="forbid")

class PredictResponse(BaseModel):
    id: str
    source: str
    text: str
    lang: str
    created_at: str
    meta: dict
    label: str
    score: float

@app.on_event("startup")
def load_model():
    global sentiment_pipeline
    sentiment_pipeline = pipeline("sentiment-analysis", model=MODEL_PATH)

@app.get("/health")
def health_check():
    return {
        "status": "ok",
        "message": "Service healthy",
        "env": APP_ENV,
        "model_path": MODEL_PATH,
    }

@app.post("/predict", response_model=PredictResponse)
def predict(payload: PredictRequest = Body(...)):
    t0 = time.perf_counter()
    try:
        t_inf = time.perf_counter()
        result = sentiment_pipeline(payload.text)[0]
        infer_ms = (time.perf_counter() - t_inf) * 1000.0
        INFER_MS.observe(infer_ms)

        # one JSON log line per prediction
        logging.info(json.dumps({
            "event": "prediction",
            "id": payload.id,
            "latency_ms": round(infer_ms, 2),
            "summary": {"label": result["label"], "score": float(result["score"])}
        }))

        SCORED.inc()
        REQS.labels("/predict", "POST", "200").inc()
        REQ_MS.observe((time.perf_counter() - t0) * 1000.0)

        return {
            "id": payload.id,
            "source": payload.source,
            "text": payload.text,
            "lang": payload.lang,
            "created_at": payload.created_at,
            "meta": payload.meta,
            "label": result["label"],
            "score": float(result["score"]),
        }
    except Exception:
        REQS.labels("/predict", "POST", "500").inc()
        logging.exception("inference_failed")
        raise HTTPException(status_code=500, detail="inference_failed")

@app.get("/metrics")
def metrics():
    return PlainTextResponse(generate_latest(), media_type=CONTENT_TYPE_LATEST)
