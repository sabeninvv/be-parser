from typing import Optional, Set
from dataclasses import dataclass
import base64


@dataclass(frozen=False)
class Match:
    uri: Optional[str] = None
    bookmakers: Optional[Set[str]] = None

    def __post_init__(self):
        if all([self.uri is not None, self.bookmakers is not None]):
            row = f"{self.uri}|{''.join(sorted(self.bookmakers))}"
            self.hash = base64.urlsafe_b64encode(row.encode("UTF-8")).decode("UTF-8")
