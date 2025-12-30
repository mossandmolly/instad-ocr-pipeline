import os
from flask import Flask, jsonify, request
from pipeline import run_pipeline

app = Flask(__name__)

# Simple shared secret (optional, but recommended)
# Set this as an env var in Cloud Run and in Scheduler header/query
API_SECRET = os.environ.get("API_SECRET", "")

@app.get("/")
def health():
    return "ok", 200

@app.post("/run")
def run():
    # Optional auth check
    if API_SECRET:
        got = request.headers.get("X-API-SECRET", "")
        if got != API_SECRET:
            return jsonify({"error": "unauthorized"}), 401

    result = run_pipeline()
    return jsonify(result), 200
