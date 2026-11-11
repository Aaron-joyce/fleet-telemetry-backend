import time


class BrokerConnectionState:
    """Thread-safe and coroutine-safe broker connection state.

    In CPython, boolean assignment is atomic at the bytecode level,
    and single-event-loop cooperative multitasking ensures coroutines
    do not interleave between await points, so explicit locks are not required.
    """

    def __init__(self) -> None:
        self._connected: bool = False
        self._reconnected_at: float | None = None  # monotonic time of last reconnect

    def set_connected(self, connected: bool) -> None:
        if connected and not self._connected:
            self._reconnected_at = time.monotonic()
        self._connected = connected

    @property
    def is_connected(self) -> bool:
        return self._connected

    @property
    def reconnected_at(self) -> float | None:
        return self._reconnected_at


broker_state = BrokerConnectionState()
