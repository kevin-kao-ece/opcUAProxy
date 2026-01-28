import os
import asyncio
from datetime import datetime
from opcua import ua, Server
from dotenv import load_dotenv
from logHelper import logger

# Globals for server state
server = Server()
node_map = {}
modbus_handlers = {}

# Globals for interaction with Web/Main
loop, ws_manager, tag_cache, cache_lock = None, None, None, None

def set_ws(loop_, ws_mgr_, tag_cache_, cache_lock_):
    global loop, ws_manager, tag_cache, cache_lock
    loop, ws_manager, tag_cache, cache_lock = loop_, ws_mgr_, tag_cache_, cache_lock_

def set_handlers(handlers_dict):
    """Links the Modbus TCP/RTU instances from main.py"""
    global modbus_handlers
    modbus_handlers = handlers_dict

def push_ws(nodeid, payload):
    if loop and ws_manager:
        loop.call_soon_threadsafe(
            lambda: asyncio.create_task(ws_manager.broadcast({nodeid: payload}))
        )

class CertificateHandler:
    def __init__(self, auto_accept=False):
        self.auto_accept = auto_accept

    def verify_certificate(self, cert):
        """Validates incoming client certificates based on config settings."""
        if self.auto_accept:
            logger.info("Security: Auto-accepting client certificate.")
            return None # None indicates success/trust
        
        # In a strict environment, you would validate the cert against a trust store here
        logger.warning("Security: Client certificate rejected (auto_accept is False).")
        return ua.StatusCode(ua.StatusCodes.BadCertificateUntrusted)

class WriteHandler:
    def datachange_notification(self, node, val, data):
        node_str = node.nodeid.to_string()
        
        # 1. Check if value actually changed (ignore echo from polling)
        with cache_lock:
            cached = tag_cache.get(node_str, {})
            if cached.get("value") == val:
                return 

        # 2. Get node configuration
        m = node_map.get(node)
        if not m or m["function"] == "input": 
            return

        # 3. Identify correct Modbus Handler (TCP or RTU)
        slave_name = m["slave"]
        handler = modbus_handlers.get(slave_name)
        
        if not handler:
            logger.error(f"No handler found for slave: {slave_name}")
            return

        try:
            # 4. Perform Modbus Write
            handler.write(m, val)
            
            # 5. Update Web UI and Cache
            payload = {
                "name": node.get_display_name().Text,
                "value": val,
                "time": datetime.now().strftime("%H:%M:%S"),
                "dir": "write",
                "status": "online"
            }
            with cache_lock:
                tag_cache[node_str] = payload
            push_ws(node_str, payload)
            
        except Exception as e:
            logger.error(f"Modbus Write Error for {node_str}: {e}")

def init_nodes(cfg):
    load_dotenv()

    """Sets up the server, security policies, and variables."""
    server.set_endpoint(cfg["opcua"]["endpoint"])
    server.set_server_name("NeoEdgeSecureGateway")

    # --- SECURITY CONFIGURATION ---
    cert_file = os.getenv("CERT_PATH", "server_cert.pem")
    key_file = os.getenv("KEY_PATH", "server_key.pem")
    env_val = os.getenv("AUTO_ACCEPT_CERTS", "false").lower()
    auto_accept = env_val in ("true", "1", "yes", "on")

    # Load Certificates
    if os.path.exists(cert_file) and os.path.exists(key_file):
        server.load_certificate(cert_file)
        server.load_private_key(key_file)
        
        # Setup Certificate Validation Logic
        cert_handler = CertificateHandler(auto_accept=auto_accept)
        server.certificate_validator = cert_handler.verify_certificate
        logger.info(f"Certificates loaded. Auto-accept is {auto_accept}")
    else:
        logger.warning("Security certificates missing! Encrypted policies will not work.")

    # Enable requested Security Policies
    server.set_security_policy([
        ua.SecurityPolicyType.NoSecurity,
        ua.SecurityPolicyType.Basic128Rsa15_SignAndEncrypt,
        ua.SecurityPolicyType.Basic256_SignAndEncrypt,
        ua.SecurityPolicyType.Basic256Sha256_SignAndEncrypt,
        # Manually adding the URIs for the newer policies to avoid the Enum error
        "http://opcfoundation.org/UA/SecurityPolicy#Aes128_Sha256_RsaOaep",
        "http://opcfoundation.org/UA/SecurityPolicy#Aes256_Sha256_RsaPss"
    ])

    # User Authentication    
    
    userCredential = os.getenv("OPC_UA_USER")
    user, pwd = userCredential.split(":",1)
    def user_auth(isess, u, p):
            return u == user and p == pwd
    server.set_security_IDs(["Username"])
    server.user_manager.set_user_manager(user_auth)

    ns = server.register_namespace(cfg["opcua"]["namespace"])
    objects = server.get_objects_node()
    
    ua_types = {
        "int16": ua.VariantType.Int16, "uint16": ua.VariantType.UInt16,
        "int32": ua.VariantType.Int32, "uint32": ua.VariantType.UInt32,
        "float": ua.VariantType.Float, "double": ua.VariantType.Double,
        "bool": ua.VariantType.Boolean, "string": ua.VariantType.String
    }

    local_map = {}
    for n in cfg["nodes"]:
        m = n["modbus"]
        ua_type = ua_types[m["datatype"]]
        init_val = "" if m["datatype"] == "string" else 0
        
        # Create the node with correct type mapping
        node = objects.add_variable(
            ua.NodeId.from_string(n["node_id"]), 
            n["name"], 
            ua.Variant(init_val, ua_type)
        )
        
        if m["function"] != "input":
            node.set_writable()
            
        local_map[node] = m
    
    return local_map

def start_opcua(node_map_):
    global node_map
    node_map = node_map_
    server.start()
    
    # Subscribe to changes from OPC UA Clients for write-enabled nodes
    handler = WriteHandler()
    sub = server.create_subscription(100, handler)
    for node in node_map:
        if node_map[node]["function"] != "input":
            sub.subscribe_data_change(node)
            
    print("Secure OPC UA Server running")
    logger.info("Secure OPC UA Server running with requested policies")