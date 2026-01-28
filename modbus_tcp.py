from pymodbus.client.sync import ModbusTcpClient
from modbus_base import ModbusBase
import threading

class ModbusTCPHandler(ModbusBase):
    def __init__(self, name, slave_config):
        self.name = name
        self.client = ModbusTcpClient(slave_config["ip"], port=slave_config.get("port", 502))
        self.slave_id = slave_config.get("slave_id", 1)
        self.lock = threading.Lock()
        self.b_swap = slave_config.get("byte_swap", False)
        self.w_swap = slave_config.get("word_swap", False)

    def read(self, m):
        with self.lock:
            if not self.client.is_socket_open():
                self.client.connect()
            
            addr = m["address"] - 1
            from modbus_base import TYPE_MAP
            regs = m.get("length", 1) if m["datatype"] == "string" else TYPE_MAP[m["datatype"]][0]

            if m["function"] == "holding":
                r = self.client.read_holding_registers(addr, regs, unit=self.slave_id)
            elif m["function"] == "input":
                r = self.client.read_input_registers(addr, regs, unit=self.slave_id)
            elif m["function"] == "coil":
                r = self.client.read_coils(addr, 1, unit=self.slave_id)
            
            if r.isError(): return None
            return self.decode_response(r, m, self.b_swap, self.w_swap)
    
    def write(self, m, val):
        with self.lock:
            if not self.client.is_socket_open():
                self.client.connect()
            return self.write_value(self.client, self.slave_id, m, val, self.b_swap, self.w_swap)