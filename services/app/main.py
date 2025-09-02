import os
from fastapi import FastAPI, Body
from pydantic import BaseModel, Field
from transformers import pipeline
from typing import Any, Dict
from dotenv import load_dotenv

app = FastAPI()

# Load environment variables
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '../../.env.dev'))

MODEL_PATH = os.getenv("MODEL_PATH", "distilbert-base-uncased-finetuned-sst-2-english").strip('" ')
APP_ENV = os.getenv("APP_ENV", "dev").strip('" ')

class PredictRequest(BaseModel):
    id: str = Field(..., example="42")
    source: str = Field(..., example="curl")
    text: str = Field(..., example="I love this jeans so much")
    lang: str = Field(..., example="en")
    created_at: str = Field(..., example="2024-01-01T00:00:00Z")
    meta: dict = Field(default_factory=dict, example={"topic": "fashion"})

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
    result = sentiment_pipeline(payload.text)[0]
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
    
