import random
from uuid import uuid4


def get_uid(prefix=None):
    """
    Returns a unique identifier
    """
    randbits = str(random.getrandbits(24))
    postfix = "-".join([randbits, str(uuid4())[:8]])
    prefix = prefix or "tukio"
    return "-".join([prefix, postfix])
