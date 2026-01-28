import asyncio
import yaml
import sys
import time
import threading
from threading import Lock
from datetime import datetime

# Local module imports
import neo_opcua
import web
from logHelper import logger
from modbus_tcp import ModbusTCPHandler
from modbus_rtu import ModbusRTUHandler

# Global dictionary to store our communication instances
handlers = {}

async def main():
    tag_cache = {}
    cache_lock = Lock()
    
    # 1. Load Configuration
    try:
        with open("config.yaml", "r") as f:
            cfg = yaml.safe_load(f)
    except Exception as e:
        print(f"CRITICAL: Failed to load config.yaml: {e}")
        sys.exit(1)

    # 2. Initialize Modbus Handlers (Factory Pattern)
    # We distinguish between TCP and RTU by checking for an 'ip' key
    for name, s in cfg["modbus"]["slaves"].items():
        try:
            if "ip" in s:
                handlers[name] = ModbusTCPHandler(name, s)
                logger.info(f"Initialized Modbus TCP: {name} ({s['ip']})")
            else:
                handlers[name] = ModbusRTUHandler(name, s)
                logger.info(f"Initialized Modbus RTU: {name} ({s['port']})")
        except Exception as e:
            logger.error(f"Failed to initialize slave {name}: {e}")

    # 3. Initialize OPC UA Server
    try:
        # Link the Modbus handlers to the OPC UA module so it can perform writes
        neo_opcua.set_handlers(handlers)
        
        # Build the address space (Nodes)
        node_map = neo_opcua.init_nodes(cfg)
    except Exception as e:
        print(f"CRITICAL: OPC UA Init failed: {e}")
        logger.error(f"OPC UA Init Error: {e}")
        sys.exit(1)

    # 4. Setup Asynchronous Bridges (Web & WebSocket)
    loop = asyncio.get_running_loop()
    
    # Start the FastAPI web server
    web.start_web(tag_cache, cache_lock)
    
    # Provide the OPC UA module with the tools to talk to the Web UI
    neo_opcua.set_ws(loop, web.ws_mgr, tag_cache, cache_lock)
    
    # Start the OPC UA stack
    neo_opcua.start_opcua(node_map)

    # 5. Modbus Polling Loop (Background Thread)
    interval = cfg["modbus"].get("poll_interval", 1.0)
    
    def poll_loop():
        logger.info("Starting Modbus Polling Loop...")
        while True:
            for node, m in node_map.items():
                slave_name = m["slave"]
                handler = handlers.get(slave_name)
                node_id_str = node.nodeid.to_string()
                
                if not handler:
                    continue

                try:
                    # Perform the read (Logic inside modbus_base.py)
                    val = handler.read(m)
                    
                    if val is None:
                        raise Exception(f"Slave {slave_name} returned no data")
                    
                    # Update OPC UA internal value
                    node.set_value(val)
                    
                    # Prepare success payload for Web UI
                    payload = {
                        "name": node.get_display_name().Text, 
                        "value": val, 
                        "time": datetime.now().strftime("%H:%M:%S"), 
                        "dir": "read", 
                        "status": "online"
                    }
                except Exception as e:
                    # Prepare error payload for Web UI
                    payload = {
                        "name": node.get_display_name().Text, 
                        "value": "ERR", 
                        "time": datetime.now().strftime("%H:%M:%S"), 
                        "dir": "read", 
                        "status": "offline"
                    }
                    # Small sleep on error to prevent CPU hammering if connection is dead
                    time.sleep(0.1) 
                
                # Atomic update of the cache and broadcast via WebSocket
                with cache_lock:
                    tag_cache[node_id_str] = payload
                neo_opcua.push_ws(node_id_str, payload)
                
            # Master polling interval
            time.sleep(interval)

    # Daemon thread ensures the loop exits when the main program stops
    threading.Thread(target=poll_loop, daemon=True).start()
    
    print("NeoEdge Gateway is fully operational.")
    logger.info("Gateway fully operational.")
    
    # Keep the main asyncio loop alive
    while True:
        await asyncio.sleep(3600)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nShutdown requested by user.")
        logger.info("Gateway shutdown.")