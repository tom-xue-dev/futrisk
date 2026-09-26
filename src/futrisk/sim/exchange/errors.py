"""Error type shared by the sim-exchange components and mapped to HTTP by the control API."""

from __future__ import annotations


class SimError(Exception):
    """A request the virtual exchange refuses.

    ``status`` is the HTTP status the control API answers with; ``code`` follows the
    generic error codes of 02 §11.1 (``BAD_REQUEST``, ``NOT_FOUND``, ``CONFLICT`` ...).
    """

    def __init__(self, code: str, message: str, status: int = 400) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.status = status

    def to_dict(self) -> dict[str, object]:
        return {"ok": False, "code": self.code, "message": self.message}


def bad_request(message: str) -> SimError:
    return SimError("BAD_REQUEST", message, 400)


def not_found(message: str) -> SimError:
    return SimError("NOT_FOUND", message, 404)


def conflict(message: str) -> SimError:
    return SimError("CONFLICT", message, 409)
