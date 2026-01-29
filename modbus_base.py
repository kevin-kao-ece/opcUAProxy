import struct, yaml
from logHelper import logger

def validate_config(file_path):
    """
    Returns (True, "") if valid, (False, "Error Message") if invalid.
    """
    try:
        with open(file_path, "r") as f:
            cfg = yaml.safe_load(f)
        
        # 1. Check top-level structure
        if not all(k in cfg for k in ["modbus", "opcua", "nodes"]):
            return False, "Missing top-level keys: modbus, opcua, or nodes"

        # 2. Validate Slaves
        slaves = cfg["modbus"].get("slaves", {})
        if not slaves:
            return False, "No Modbus slaves defined"
        
        for name, s in slaves.items():
            if "ip" not in s and "port" not in s:
                return False, f"Slave '{name}' needs an 'ip' (TCP) or 'port' (RTU)"

        # 3. Validate Nodes
        for node in cfg["nodes"]:
            required_node_keys = ["node_id", "name", "modbus"]
            if not all(k in node for k in required_node_keys):
                return False, f"Node {node.get('name', 'unknown')} missing required keys"
            
            m = node["modbus"]
            if m["slave"] not in slaves:
                return False, f"Node '{node['name']}' references undefined slave '{m['slave']}'"
            
            if m["datatype"] not in ["int16", "uint16", "int32", "uint32", "float", "double", "bool", "string"]:
                return False, f"Invalid datatype '{m['datatype']}' in node '{node['name']}'"

        return True, ""
    except Exception as e:
        return False, f"YAML Syntax Error: {str(e)}"

TYPE_MAP = {
    "int16": (1, "h"), "uint16": (1, "H"),
    "int32": (2, "i"), "uint32": (2, "I"),
    "float": (2, "f"), "double": (4, "d"),
    "bool":  (1, "?"), "string": (None, "s")
}

class ModbusBase:
    def handle_swaps(self, raw_bytes, byte_swap, word_swap):
        data = bytearray(raw_bytes)
        if byte_swap:
            for i in range(0, len(data), 2):
                if i + 1 < len(data):
                    data[i], data[i+1] = data[i+1], data[i]
        if word_swap and len(data) >= 4:
            words = [data[i:i+2] for i in range(0, len(data), 2)]
            words.reverse()
            data = bytearray().join(words)
        return bytes(data)

    def decode_response(self, r, m, b_swap, w_swap):
        dtype = m["datatype"]
        # Handle Coils
        if m["function"] == "coil":
            return bool(r.bits[0])
            
        # Handle Registers
        raw = b"".join(x.to_bytes(2, "big") for x in r.registers)
        raw = self.handle_swaps(raw, b_swap, w_swap)
        
        if dtype == "string":
            return raw.decode('utf-8', errors='ignore').strip('\x00')
        
        return struct.unpack(">" + TYPE_MAP[dtype][1], raw)[0]
    
    def write_value(self, client, slave_id, m, val, b_swap, w_swap):
        addr = m["address"] - 1
        dtype = m["datatype"]

        # 1. Handle Coils
        if m["function"] == "coil":
            return client.write_coil(addr, bool(val), unit=slave_id)

        # 2. Handle Registers (Holding)
        if dtype == "string":
            # Strings must be padded to the correct length (2 bytes per register)
            raw = str(val).encode('utf-8').ljust(m["length"] * 2, b'\x00')
        else:
            # Pack numbers/booleans using the TYPE_MAP
            raw = struct.pack(">" + TYPE_MAP[dtype][1], val)

        # 3. Apply Swaps
        raw = self.handle_swaps(raw, b_swap, w_swap)

        # 4. Convert bytes back to register list [int16, int16...]
        regs = [int.from_bytes(raw[i:i+2], "big") for i in range(0, len(raw), 2)]
        
        return client.write_registers(addr, regs, unit=slave_id)