import asyncio
from threading import Lock
import modbus, neo_opcua, web
import sys

async def main():
    tag_cache = {}
    cache_lock = Lock()
    
    # 1. Initialize Nodes from Config
    try:
        node_map = neo_opcua.init_nodes()
    except Exception as e:
        print(f"CRITICAL: Failed to load nodes: {e}")
        sys.exit(1)

    loop = asyncio.get_running_loop()

    # 2. Start Services
    web.start_web(tag_cache, cache_lock)
    neo_opcua.set_ws(loop, web.ws_mgr, tag_cache, cache_lock)
    neo_opcua.start_opcua(node_map)

    # 3. Initial Modbus Sync
    print("Performing initial Modbus read...")
    for node, m in node_map.items():
        val = modbus.read_modbus(m)
        if val is not None:
            node.set_value(val)
            tag_cache[node.nodeid.to_string()] = {
                "name": node.get_display_name().Text,
                "value": val, "time": "init", "dir": "read"
            }

    # 4. Start Background Polling
    interval = modbus.cfg.get("poll_interval", 1.0)
    modbus.start_polling(node_map, tag_cache, cache_lock, neo_opcua.push_ws, interval)

    print("Gateway is fully operational.")
    
    # Keep the async loop running until the process is killed
    while True:
        await asyncio.sleep(3600)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Shutdown requested by user.")