import json
from unittest.mock import AsyncMock, patch
import pytest

from robot.publisher import SESSION_ID, start_publisher, run_publisher_sync


@pytest.mark.asyncio
async def test_publisher_stamps_session_id():
    """Test publisher embeds SESSION_ID in every published MQTT payload."""
    events = [
        {"x": 1.0, "y": 2.0, "t": 0.0},
        {"x": 2.0, "y": 3.0, "t": 0.1},
    ]

    mock_client = AsyncMock()
    mock_client.__aenter__.return_value = mock_client
    mock_client.__aexit__.return_value = None

    with patch("aiomqtt.Client", return_value=mock_client):
        with patch("asyncio.sleep", new_callable=AsyncMock):
            await start_publisher("robot_001", events, multiplier=100.0)

    assert mock_client.publish.call_count == 2
    for call in mock_client.publish.call_args_list:
        _, kwargs = call
        payload = json.loads(kwargs["payload"])
        assert "session_id" in payload
        assert payload["session_id"] == SESSION_ID
