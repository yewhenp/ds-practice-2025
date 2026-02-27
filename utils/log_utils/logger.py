import logging

_logger = None

def setup_logger(service_name):
    global _logger

    if _logger is not None:
        return _logger

    _logger = logging.getLogger(service_name)

    _logger.setLevel(logging.INFO)
    
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    formatter = logging.Formatter(f"%(asctime)s - {service_name} - %(levelname)s - %(message)s")
    ch.setFormatter(formatter)

    fh = logging.FileHandler(f"/app/logs/{service_name}.log")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(formatter)

    _logger.addHandler(ch)
    _logger.addHandler(fh)

    return _logger