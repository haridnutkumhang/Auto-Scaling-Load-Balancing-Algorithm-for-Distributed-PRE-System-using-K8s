from fastapi import FastAPI, UploadFile, File, HTTPException
import psutil
import shutil
import subprocess
import os

print("[proxy-worker] Startup — worker_app.py loaded")

app = FastAPI()

@app.post("/reencrypt")
async def reencrypt(file: UploadFile = File(...)):
    print(f"[proxy-worker][reencrypt] Received file: {file.filename}")
    """
    Receive an uploaded Python file, execute it, 
    then return stdout/stderr plus post-execution CPU & RAM usage.
    """
    # 1. Save the uploaded file to /tmp
    filename = file.filename
    if not filename.endswith(".py"):
        filename += ".py"
    tmp_path = os.path.join("/tmp", filename)
    try:
        with open(tmp_path, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to save file: {e}")

    # 2. Execute it with subprocess
    try:
        proc = subprocess.run(
            ["python", tmp_path],
            capture_output=True,
            text=True,
            cwd="/tmp"
        )
    except Exception as e:
        os.remove(tmp_path)
        raise HTTPException(status_code=500, detail=f"Execution error: {e}")

    # 3. Clean up the temp file
    os.remove(tmp_path)

    # 4. Return everything as JSON
    return {
        "returncode": proc.returncode,
        "stdout": proc.stdout,
        "stderr": proc.stderr,
    }
