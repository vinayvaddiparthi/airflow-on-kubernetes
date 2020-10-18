from .base_column import BaseColumn


class FixedWidthColumn(BaseColumn):
    def __init__(self, width: int, padding: str = " "):
        super().__init__()
        self._width = width
        self._padding = padding

    def __str__(self) -> str:
        adjusted = self.value
        # Truncate to fixed width if greater than width
        adjusted = adjusted[: self._width] if len(adjusted) > self._width else adjusted
        # Pad to fixed width if less than width
        adjusted = adjusted.ljust(self._width, self._padding)
        return adjusted
