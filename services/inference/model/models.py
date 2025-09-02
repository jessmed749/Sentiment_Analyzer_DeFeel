from transformers import pipeline

sentiment = None

def get_sentiment_pipeline():
    global sentiment
    if sentiment is None:
        sentiment = pipeline(
            "sentiment-analysis", 
            model="distilbert-base-uncased-finetuned-sst-2-english"                 )
        
    return sentiment