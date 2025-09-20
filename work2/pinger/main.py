import os
import sys
import time
import logging
import concurrent.futures
import psycopg
import re

INTERVAL_SECONDS = int(os.getenv("POLL_INTERVAL_SECONDS"))
POLL_TIMEOUT_SECONDS = int(os.getenv("POLL_TIMEOUT_SECONDS"))

DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "mydb")

LOG_FILE = os.getenv("LOG_FILE", None)

if not DB_USER or not DB_PASSWORD:
    print("ERROR: DB_USER and DB_PASSWORD must be set", file=sys.stderr)
    sys.exit(1)

logger = logging.getLogger("pinger")
logger.setLevel(logging.DEBUG)

stdout_handler = logging.StreamHandler(sys.stdout)
stdout_handler.setLevel(logging.INFO)
logger.addHandler(stdout_handler)

stderr_handler = logging.StreamHandler(sys.stderr)
stderr_handler.setLevel(logging.ERROR)
logger.addHandler(stderr_handler)

if LOG_FILE:
    fh = logging.FileHandler(LOG_FILE, encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    logger.addHandler(fh)

TYPICAL_RE = re.compile(r"^PostgreSQL", re.IGNORECASE)

def check_db_version():
    try:
        with psycopg.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            connect_timeout=min(POLL_TIMEOUT_SECONDS, 10)
        ) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT VERSION();")
                row = cur.fetchone()
                version = row[0] if row else None
                atypical = version and not TYPICAL_RE.match(version)
                return {"ok": True, "version": version, "atypical": atypical}
    except Exception as e:
        return {"ok": False, "error": str(e)}

def run_single_check():
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as ex:
        future = ex.submit(check_db_version)
        try:
            result = future.result(timeout=POLL_TIMEOUT_SECONDS)
        except concurrent.futures.TimeoutError:
            logger.error("Connection timed out after %s seconds", POLL_TIMEOUT_SECONDS)
            return

    if not result["ok"]:
        logger.error("DB connection failed: %s", result["error"])
    else:
        if result["atypical"]:
            logger.info("ATYPICAL version: %s", result["version"])
        else:
            logger.info("DB version: %s", result["version"])

def main_loop():
    logger.info("Pinger started. Interval=%s sec", INTERVAL_SECONDS)
    while True:
        start = time.time()
        run_single_check()
        elapsed = time.time() - start
        time.sleep(max(0, INTERVAL_SECONDS - elapsed))

if __name__ == "__main__":
    main_loop()
