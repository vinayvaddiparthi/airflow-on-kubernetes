class FixedWidthColumn:
    def __init__(self, width: int, padding=" "):
        self._width = width
        self._padding = padding
        self._set_value(self._padding)

    def _set_value(self, value):
        adjusted = str(value)
        # Truncate to fixed width if greater than width
        adjusted = adjusted[: self._width] if len(adjusted) > self._width else adjusted
        # Pad to fixed width if less than width
        adjusted = adjusted.ljust(self._width, self._padding)
        self._value = adjusted

    def set(self, value):
        self._set_value(value)

    def __str__(self):
        return self._value
