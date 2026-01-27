import struct
import threading
import time
import yaml
from pymodbus.client.sync import ModbusTcpClient
from datetime import datetime

cfg = yaml.safe_load(open("config.yaml"))

TYPE_MAP = {
    "int16":  (1, "h"),
    "uint16": (1, "H"),
    "int32":  (2, "i"),
    "uint32": (2, "I"),
    "float":  (2, "f"),
    "double": (4, "d"),
    "bool":   (1, "?"),
}

# ---------------- MODBUS CLIENTS & LOCKS ----------------
modbus_clients = {}
client_locks = {} # Critical for thread safety

for name, s in cfg["modbus"]["slaves"].items():
    c = ModbusTcpClient(s["ip"], port=s["port"])
    c.connect()
    modbus_clients[name] = (c, s["unit_id"])
    client_locks[name] = threading.Lock()

def validate_config_file(filepath):
    """Checks if the YAML is valid and contains required keys."""
    try:
        with open(filepath, 'r') as f:
            data = yaml.safe_load(f)
        
        # Basic Structural Check
        required = ["opcua", "modbus", "nodes"]
        for key in required:
            if key not in data:
                return False, f"Missing required section: {key}"
        
        # Validate node structure
        for node in data["nodes"]:
            if "modbus" not in node or "address" not in node["modbus"]:
                return False, f"Node {node.get('name')} is missing Modbus mapping"
                
        return True, "Valid"
    except Exception as e:
        return False, str(e)

def read_modbus(m):
    slave_name = m["slave"]
    client, unit = modbus_clients[slave_name]
    regs, fmt = TYPE_MAP[m["datatype"]]
    addr = m["address"] - 1

    with client_locks[slave_name]:
        if m["function"] == "holding":
            r = client.read_holding_registers(addr, regs, unit=unit)
            if r.isError(): return None
            raw = b"".join(x.to_bytes(2, "big") for x in r.registers)
            return struct.unpack(">" + fmt, raw)[0]
        
        if m["function"] == "coil":
            r = client.read_coils(addr, 1, unit=unit)
            if r.isError(): return None
            return r.bits[0]
    return None

def write_modbus(m, val):
    slave_name = m["slave"]
    client, unit = modbus_clients[slave_name]
    addr = m["address"] - 1

    with client_locks[slave_name]:
        if m["function"] == "coil":
            client.write_coil(addr, bool(val), unit=unit)
        else:
            _, fmt = TYPE_MAP[m["datatype"]]
            raw = struct.pack(">" + fmt, val)
            regs = [int.from_bytes(raw[i:i+2], "big") for i in range(0, len(raw), 2)]
            client.write_registers(addr, regs, unit=unit)

# ---------------- POLLING ----------------
def poll_loop(node_map, tag_cache, cache_lock, push_ws, interval):
    while True:
        for node, m in node_map.items():
            val = read_modbus(m)
            if val is None: continue
            
            # 1. Update OPC UA Server
            # We use a custom attribute or just check value to prevent feedback loops in neo_opcua.py
            node.set_value(val)

            # 2. Update Cache & Notify Web
            payload = {
                "name": node.get_display_name().Text,
                "value": val,
                "time": datetime.now().strftime("%H:%M:%S"),
                "dir": "read"
            }
            with cache_lock:
                tag_cache[node.nodeid.to_string()] = payload
            
            push_ws(node.nodeid.to_string(), payload)
            
        time.sleep(interval)

def start_polling(node_map, tag_cache, cache_lock, push_ws, interval=1.0):
    t = threading.Thread(
        target=poll_loop, 
        args=(node_map, tag_cache, cache_lock, push_ws, interval), 
        daemon=True
    )
    t.start()