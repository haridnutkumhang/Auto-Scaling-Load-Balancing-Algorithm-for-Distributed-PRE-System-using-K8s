import os
import re
import time
import asyncio
import aiohttp
from fastapi import FastAPI, UploadFile, File, HTTPException
from kubernetes import client, config
from kubernetes.config.config_exception import ConfigException

app = FastAPI()

# ── Kubernetes client setup ────────────────────────────────────────────────────
try:
    config.load_incluster_config()
    print("[master] Loaded in-cluster Kubernetes config")
except ConfigException:
    config.load_kube_config()
    print("[master] Loaded local kubeconfig")

metrics_api = client.CustomObjectsApi()
core_api    = client.CoreV1Api()

# ── aiohttp client session (to reuse connections) ──────────────────────────────
session: aiohttp.ClientSession

@app.on_event("startup")
async def startup_event():
    global session
    session = aiohttp.ClientSession()

@app.on_event("shutdown")
async def shutdown_event():
    await session.close()

# ── Helpers to parse CPU & memory strings ───────────────────────────────────────
def parse_cpu(cpu_str: str) -> float:
    if cpu_str.endswith("n"):
        return float(cpu_str[:-1]) / 1_000_000_000
    if cpu_str.endswith("m"):
        return float(cpu_str[:-1]) / 1000.0
    return float(cpu_str)

def parse_mem(mem_str: str) -> float:
    unit = mem_str[-2:]
    value = float(mem_str[:-2])
    if unit == "Ki":
        return value / 1024.0
    if unit == "Gi":
        return value * 1024.0
    # assume Mi
    return value

# ── Fetch metrics from Metrics API ─────────────────────────────────────────────
def get_all_worker_metrics() -> dict:
    try:
        data = metrics_api.list_cluster_custom_object(
            group="metrics.k8s.io", version="v1beta1", plural="pods"
        )
    except client.exceptions.ApiException as e:
        raise HTTPException(status_code=500, detail=f"Metrics API error: {e}")

    m = {}
    for item in data["items"]:
        name = item["metadata"]["name"]
        if re.match(r"proxy-worker-.*", name):
            cpu = parse_cpu(item["containers"][0]["usage"]["cpu"])
            mem = parse_mem(item["containers"][0]["usage"]["memory"])
            m[name] = (cpu, mem)
    return m

# ── Choose the least-loaded worker ──────────────────────────────────────────────
def choose_best_worker() -> str:
    metrics = get_all_worker_metrics()
    if not metrics:
        raise HTTPException(status_code=503, detail="No worker pods available")
    best_name = min(metrics.items(), key=lambda kv: (kv[1][0], kv[1][1]))[0]
    # get Pod IP directly
    pod = core_api.read_namespaced_pod(best_name, namespace="default")
    return f"http://{pod.status.pod_ip}:8080"

# ── Re-encrypt endpoint ─────────────────────────────────────────────────────────
@app.post("/reencrypt")
async def reencrypt(file: UploadFile = File(...)):
    # 1. pick worker
    try:
        worker_url = choose_best_worker()
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error choosing worker: {e}")

    # 2. save upload
    tmp_path = "/tmp/temp.py"
    try:
        content = await file.read()
        with open(tmp_path, "wb") as buf:
            buf.write(content)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to save file: {e}")

    # 3. forward asynchronously
    try:
        form = aiohttp.FormData()
        form.add_field(
            name="file",
            value=open(tmp_path, "rb"),
            filename=os.path.basename(tmp_path),
            content_type="application/octet-stream"
        )
        async with session.post(f"{worker_url}/reencrypt", data=form, timeout=None) as resp:
            resp.raise_for_status()
            result = await resp.json()
    except aiohttp.ClientError as e:
        raise HTTPException(status_code=502, detail=f"Worker error: {e}")
    finally:
        try: os.remove(tmp_path)
        except OSError: pass

    return result
