from rubymarshal.reader import loads as rubymarshal_loads


def marshalled(func):
    def unmarshal(*args):
        marshaled_value = func(*args)
        unmarshalled_value = (
            rubymarshal_loads(marshaled_value) if marshaled_value else None
        )
        return unmarshalled_value

    return unmarshal
