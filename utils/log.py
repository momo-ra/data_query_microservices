# app/utils/logger.py
import logging

class CustomLogger(logging.Logger):
    def danger(self, message, *args, **kwargs):
        self.error(f"❌ {message}", *args, **kwargs)
    
    def success(self, message, *args, **kwargs):
        self.info(f"✅ {message}", *args, **kwargs)
    
    def warn_custom(self, message, *args, **kwargs):
        self.warning(f"⚠️ {message}", *args, **kwargs)

def setup_logger(name: str):
    logging.setLoggerClass(CustomLogger)
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s || %(name)s || %(levelname)s || %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    
    return logger