from __future__ import annotations

from enzyextract.post.decode import decode_openai_batch


def test_openai_error_response_is_a_terminal_row_not_a_crash():
    frame = decode_openai_batch([{
        "custom_id": "run_v1_D1",
        "response": {"status_code": 401, "body": None, "error": "invalid_api_key"},
    }])
    assert frame.height == 1
    assert frame["custom_id"][0] == "run_v1_D1"
    assert frame["status_code"][0] == 401
    assert frame["finish_reason"][0] == "error"
    assert frame["content"][0] is None
    assert frame["error"][0] == "invalid_api_key"
