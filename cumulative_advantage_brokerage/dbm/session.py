from typing import List, Any

from sqlalchemy.orm import Session

class CumAdvBrokSession(Session):
    def commit_list(self, l: List[Any]):
        self.add_all(l)
        self.commit()
        for el in l:
            self.refresh(el)
