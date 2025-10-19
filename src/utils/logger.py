from loguru import logger
import sys
import os

LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)

# Remove default handler
logger.remove()

# Add console + file sinks
FORMAT = "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level}</level> | <cyan>{extra[step]}</cyan> | {message}"
logger.add(sys.stdout, colorize=True, format=FORMAT)
logger.add(
    f"{LOG_DIR}/pipeline.log",
    retention="15 days",
    level="INFO",
    rotation="10 MB",
    format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {extra[step]} | {message}"
)


