import logging, sys

def get_logger(name='hca'):
    l=logging.getLogger(name)
    if not l.handlers:
        h=logging.StreamHandler(sys.stdout); l.addHandler(h); l.setLevel(logging.INFO)
    return l
