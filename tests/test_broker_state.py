from app.broker_state import BrokerConnectionState


def test_set_connected_records_reconnect_time():
    """Test A: set_connected(True) records reconnect time."""
    state = BrokerConnectionState()
    assert state.is_connected is False
    assert state.reconnected_at is None

    state.set_connected(True)
    assert state.is_connected is True
    assert state.reconnected_at is not None


def test_set_disconnected_preserves_reconnected_at():
    """Test B: set_connected(False) clears connected flag but preserves reconnected_at."""
    state = BrokerConnectionState()
    state.set_connected(True)
    assert state.is_connected is True
    t1 = state.reconnected_at
    assert t1 is not None

    state.set_connected(False)
    assert state.is_connected is False
    assert state.reconnected_at == t1  # still set from previous connect


def test_repeated_set_connected_does_not_update_reconnected_at():
    """Test C: Repeated set_connected(True) does not update reconnected_at."""
    state = BrokerConnectionState()
    state.set_connected(True)
    t1 = state.reconnected_at
    assert t1 is not None

    state.set_connected(True)  # already connected
    assert state.reconnected_at == t1  # unchanged
