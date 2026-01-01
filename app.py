import os
from flask import Flask, request, jsonify

from pipeline import run_pipeline  # adjust if your import path differs

app = Flask(__name__)

# Optional shared secret (recommended if you're calling from Scheduler)
# Set this env var in Cloud Run, and send header X-API-SECRET from caller
API_SECRET = os.environ.get("API_SECRET", "")


@app.get("/")
def health():
    return "OK", 200


@app.post("/run")
def run():
    # Optional auth check
    if API_SECRET:
        got = request.headers.get("X-API-SECRET", "")
        if got != API_SECRET:
            return jsonify({"error": "unauthorized"}), 401

    try:
        result = run_pipeline()
        # If your run_pipeline returns None or a string, normalize it
        if result is None:
            result = {"status": "triggered"}
        elif isinstance(result, str):
            result = {"status": result}
        return jsonify(result), 200

    except Exception as e:
        print("ERROR running pipeline:", str(e))
        return jsonify({"error": str(e)}), 500
