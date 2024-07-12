from .session import CumAdvBrokSession

class HasSession:
    session: CumAdvBrokSession
    def __init__(
            self, *arg, session: CumAdvBrokSession, **kwargs) -> None:
        self.session = session
        super().__init__(*arg, **kwargs)
