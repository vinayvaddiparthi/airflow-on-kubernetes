import random
import string


def random_identifier() -> str:
    return "".join(random.choice(string.ascii_uppercase) for _ in range(36))  # nosec
