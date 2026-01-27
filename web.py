from fastapi import FastAPI, WebSocket, WebSocketDisconnect, UploadFile, File, HTTPException
from fastapi.responses import HTMLResponse
import os, sys, shutil, uvicorn, threading, asyncio
import modbus

class WSManager:
    def __init__(self):
        self.clients = set()

    async def connect(self, ws):
        await ws.accept()
        self.clients.add(ws)

    def disconnect(self, ws):
        self.clients.discard(ws)

    async def broadcast(self, msg):
        if not self.clients: return
        # Send to all connected browsers simultaneously
        await asyncio.gather(*[client.send_json(msg) for client in self.clients], return_exceptions=True)

app = FastAPI()
ws_mgr = WSManager()
tag_cache, cache_lock = None, None

# --- RESTORED ROOT PATH ---
@app.get("/", response_class=HTMLResponse)
async def get_index():
    # Use absolute path to prevent "File not found" errors
    path = os.path.join(os.path.dirname(__file__), "index.html")
    with open(path, "r") as f:
        return f.read()

@app.post("/upload_config")
async def upload_config(file: UploadFile = File(...)):
    temp_path = "config_temp.yaml"
    final_path = "config.yaml"
    
    # 1. Save to temporary file first
    with open(temp_path, "wb") as f:
        content = await file.read()
        f.write(content)
    
    # 2. Validate the temporary file
    is_valid, error_msg = modbus.validate_config_file(temp_path)
    if not is_valid:
        os.remove(temp_path)
        raise HTTPException(status_code=400, detail=f"Invalid Config: {error_msg}")
    
    # 3. If valid, overwrite the real config
    shutil.move(temp_path, final_path)
    return {"status": "Config updated and validated"}

@app.post("/restart")
async def restart_gateway():
    print("Restarting gateway process...")
    
    def delayed_restart():
        import time
        time.sleep(1) # Give the browser 1 second to receive the "OK"
        os.execv(sys.executable, [sys.executable] + sys.argv)

    # Run restart in a separate thread so this request can finish
    threading.Thread(target=delayed_restart).start()
    return {"status": "restarting"}

@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await ws_mgr.connect(ws)
    # Send current state immediately upon connection
    with cache_lock:
        await ws.send_json(tag_cache)
    try:
        while True:
            # Keep the connection alive
            await ws.receive_text() 
    except WebSocketDisconnect:
        ws_mgr.disconnect(ws)

def start_web(cache, lock, host="0.0.0.0", port=8080):
    global tag_cache, cache_lock
    tag_cache, cache_lock = cache, lock
    # Run Uvicorn in a daemon thread so it doesn't block the main gateway logic
    threading.Thread(target=lambda: uvicorn.run(app, host=host, port=port, log_level="error"), daemon=True).start()