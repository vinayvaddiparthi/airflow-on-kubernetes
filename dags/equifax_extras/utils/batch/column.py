from .base_column import BaseColumn


class Column(BaseColumn):
    def __str__(self) -> str:
        return self.value
