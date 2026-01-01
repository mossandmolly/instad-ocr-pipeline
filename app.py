import os
import traceback
from flask import Flask, request, jsonify

from pipeline import run_pipeline  # adjust if needed

app = Flask(__name__)

# Optional shared secret (recommended for Scheduler)
# Set this in Cloud Run → Variables & Secrets
API_SECRET = os.environ.get("API_SECRET", "")

print("🚀 app.py loaded successfully")


@app.route("/", methods=["GET"])
def health():
    return "OK", 200


@app.route("/run", methods=["POST"])
def run():
    # ---- request metadata (VERY IMPORTANT FOR DEBUGGING) ----
    print("🔔 /run invoked")
    print("User-Agent:", request.headers.get("User-Agent"))
    print("Headers:", dict(request.headers))

    # ---- optional auth ----
    if API_SECRET:
        got = request.headers.get("X-API-SECRET", "")
        if got != API_SECRET:
            print("❌ Unauthorized request")
            return jsonify({"error": "unauthorized"}), 401

    try:
        print("▶️ Starting pipeline...")
        result = run_pipeline()
        print("✅ Pipeline completed:", result)

        # Normalize response
        if result is None:
            payload = {"status": "ok"}
        elif isinstance(result, dict):
            payload = result
        else:
            payload = {"status": str(result)}

        return jsonify(payload), 200

    except Exception as e:
        print("🔥 ERROR running pipeline")
        traceback.print_exc()  # <-- THIS IS THE KEY FIX
        return jsonify({
            "error": str(e),
            "type": type(e).__name__
        }), 500
