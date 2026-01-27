from opcua import ua, Server
from datetime import datetime
import modbus
import asyncio

server = Server()
cfg = modbus.cfg

def init_nodes():
    server.set_endpoint(cfg["opcua"]["endpoint"])
    server.set_server_name("NeoEdgeOPCUAServer")
    ns = server.register_namespace(cfg["opcua"]["namespace"])

    # User Auth
    def user_auth(isession, username, password):
        users = cfg["opcua"]["users"]
        return users.get(username) == password
    server.set_security_IDs(["Username"])
    server.user_manager.set_user_manager(user_auth)

    objects = server.get_objects_node()
    node_map = {}

    for n in cfg["nodes"]:
        ua_type = {
            "int16": ua.VariantType.Int16, "uint16": ua.VariantType.UInt16,
            "int32": ua.VariantType.Int32, "uint32": ua.VariantType.UInt32,
            "float": ua.VariantType.Float, "double": ua.VariantType.Double,
            "bool": ua.VariantType.Boolean,
        }[n["modbus"]["datatype"]]

        node = objects.add_variable(ua.NodeId.from_string(n["node_id"]), n["name"], ua.Variant(0, ua_type))
        node.set_writable()
        node_map[node] = n["modbus"]
    
    return node_map

# ---------------- GLOBALS FOR WS ----------------
loop, ws_manager, tag_cache, cache_lock = None, None, None, None

def set_ws(loop_, ws_mgr_, tag_cache_, cache_lock_):
    global loop, ws_manager, tag_cache, cache_lock
    loop, ws_manager, tag_cache, cache_lock = loop_, ws_mgr_, tag_cache_, cache_lock_

def push_ws(nodeid, payload):
    if loop and ws_manager:
        loop.call_soon_threadsafe(lambda: asyncio.create_task(ws_manager.broadcast({nodeid: payload})))

# ---------------- WRITE HANDLER ----------------
class WriteHandler:
    def datachange_notification(self, node, val, data):
        node_str = node.nodeid.to_string()
        
        # FIX: Check if value is actually different from cache to avoid polling-write loops
        with cache_lock:
            cached = tag_cache.get(node_str, {})
            if cached.get("value") == val:
                return 

        m = node_map[node]
        try:
            modbus.write_modbus(m, val)
            print(f"Modbus Write Success: {node_str} = {val}")
        except Exception as e:
            print(f"Modbus Write Error: {e}")

        payload = {
            "name": node.get_display_name().Text,
            "value": val,
            "time": datetime.now().strftime("%H:%M:%S"),
            "dir": "write"
        }
        with cache_lock:
            tag_cache[node_str] = payload
        push_ws(node_str, payload)

def start_opcua(node_map_):
    global node_map
    node_map = node_map_
    server.start()
    handler = WriteHandler()
    sub = server.create_subscription(100, handler)
    for node in node_map:
        sub.subscribe_data_change(node)
    print("OPC UA Server running")