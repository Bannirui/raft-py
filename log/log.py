import logging
import os
from logging.handlers import RotatingFileHandler

def setup_logger(name='app', log_file='logs/app.log', level=logging.INFO, max_bytes=5 * 1024 * 1024, backup_count=5):
    """配置logger实例"""
    os.makedirs(os.path.dirname(log_file), exist_ok=True)
    logger = logging.getLogger(name)
    logger.setLevel(level)
    if logger.handlers: return logger
    # 控制台输出
    console_handler = logging.StreamHandler()
    console_format = logging.Formatter('[%(levelname)s] %(asctime)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    console_handler.setFormatter(console_format)
    logger.addHandler(console_handler)
    # 文件输出
    file_handler = RotatingFileHandler(log_file, maxBytes=max_bytes, backupCount=backup_count, encoding='utf-8')
    file_format = logging.Formatter('%(asctime)s [%(levelname)s] %(name)s: %(message)s')
    file_handler.setFormatter(file_format)
    logger.addHandler(file_handler)
    return logger