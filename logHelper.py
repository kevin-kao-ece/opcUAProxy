# logger_config.py
import logging
from dotenv import load_dotenv
from logging.handlers import TimedRotatingFileHandler
import os

class AppLogger:
    def __init__(self):
        load_dotenv()
        self.app_name = os.getenv("APP_NAME", "APP") + " API " + os.getenv("APP_VERSION", "")
        log_level_str = os.getenv("LOG_LEVEL", "INFO").upper()
        self.log_level = getattr(logging, log_level_str, logging.INFO)

        # Create log directory
        os.makedirs("logs", exist_ok=True)

        # Initialize logger
        self.logger = logging.getLogger(self.app_name)
        self.logger.setLevel(self.log_level)

        # Add handlers only once
        if not self.logger.handlers:
            self._add_console_handler()
            self._add_file_handler()
    
    def _add_console_handler(self):
        console_handler = logging.StreamHandler()
        console_handler.setLevel(self.log_level)
        console_format = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        console_handler.setFormatter(console_format)

        self.logger.addHandler(console_handler)
    
    def _add_file_handler(self):
        backup_count = int(os.getenv("LOG_FILE_COUNT", "5"))

        file_handler = TimedRotatingFileHandler(
            "logs/app.log",
            when="midnight",
            interval=1,
            backupCount=backup_count,
            encoding="utf-8"
        )
        file_handler.setLevel(self.log_level)
        file_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(file_format)

        self.logger.addHandler(file_handler)

    def get_logger(self):
        return self.logger

# Log 統一由此 Instance 管理，其他程式不應該自行建立 instance
logger = AppLogger().get_logger()
