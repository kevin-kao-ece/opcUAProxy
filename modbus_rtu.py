from pymodbus.client.sync import ModbusSerialClient
from modbus_base import ModbusBase
from logHelper import logger
import threading

class ModbusRTUHandler(ModbusBase):
    def __init__(self, name, slave_config):
        # RTU uses serial parameters
        self.name = name
        self.client = ModbusSerialClient(
            method='rtu',
            port=slave_config["port"], # e.g. '/dev/ttyUSB0'
            baudrate=slave_config.get("baudrate", 9600),
            parity=slave_config.get("parity", 'N'),
            stopbits=slave_config.get("stopbits", 1),
            bytesize=slave_config.get("databits", 8),
            timeout=1
        )
        self.slave_id = slave_config.get("slave_id", 1)
        # IMPORTANT: If multiple slaves share one serial port, they must share this lock!
        self.lock = threading.Lock() 
        self.b_swap = slave_config.get("byte_swap", False)
        self.w_swap = slave_config.get("word_swap", False)

    def read(self, m):
        with self.lock:
            try:
                # Attempt connection
                if not self.client.connect():
                    logger.error(f"RTU Error: Could not open port {self.client.port}")
                    return None
                
                addr = m["address"] - 1
                from modbus_base import TYPE_MAP
                regs = m.get("length", 1) if m["datatype"] == "string" else TYPE_MAP[m["datatype"]][0]

                # Execute Read
                if m["function"] == "holding":
                    r = self.client.read_holding_registers(addr, regs, unit=self.slave_id)
                elif m["function"] == "input":
                    r = self.client.read_input_registers(addr, regs, unit=self.slave_id)
                elif m["function"] == "coil":
                    r = self.client.read_coils(addr, 1, unit=self.slave_id)
                else:
                    return None

                # Check for Modbus protocol errors (e.g., CRC fail, Timeout, Illegal Address)
                if r is None or r.isError():
                    # r is None usually means a timeout occurred
                    error_msg = f"RTU Read Error on {self.name}: {r}"
                    logger.warning(error_msg)
                    return None

                return self.decode_response(r, m, self.b_swap, self.w_swap)

            except Exception as e:
                # This catches hardware level errors like serial.serialutil.SerialException
                logger.error(f"RTU Critical Hardware Error on {self.client.port}: {e}")
                return None
    
    def write(self, m, val):
        with self.lock:
            if not self.client.is_socket_open():
                self.client.connect()
            return self.write_value(self.client, self.slave_id, m, val, self.b_swap, self.w_swap)