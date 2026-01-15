import logging
import sys
import colorlog

# define log format
# %(log_color)s - sets the color based on the log level
log_format = "%(log_color)s%(asctime)s [%(levelname)s] %(message)s"

# colorlog ColoredFormatter
formatter = colorlog.ColoredFormatter(
    log_format,
    datefmt='%Y-%m-%d %H:%M:%S',
    reset=True,
    log_colors={
        'DEBUG':    'cyan',
        'INFO':     'white',
        'WARNING':  'yellow',
        'ERROR':    'red',
        'CRITICAL': 'bold_red,bg_white',
    }
)

# handler for stdout
handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(formatter)

# logging configuration
logging.basicConfig(
    level=logging.INFO,
    handlers=[handler]  # use custom handler with color formatter
)

# =================================================================
# Set log level for external libraries to WARNING to reduce verbosity
# =================================================================
logging.getLogger("azure").setLevel(logging.WARNING)
logging.getLogger("azure.core.pipeline.policies.http_logging_policy").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)

# custom logger
logger = logging.getLogger("AzureSnowflakePipeline")