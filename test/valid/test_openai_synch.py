from __future__ import annotations

from types import SimpleNamespace

import httpx
from openai import AuthenticationError

from enzyextract.submit import openai_synch


class FakeChatCompletions:
    def __init__(self):
        self.calls = []

    def create(self, **kwargs):
        self.calls.append(kwargs)
        usage = SimpleNamespace(prompt_tokens=3, completion_tokens=2, total_tokens=5)
        message = SimpleNamespace(role="assistant", content="OK")
        choice = SimpleNamespace(message=message, finish_reason="stop")
        return SimpleNamespace(
            id="chatcmpl_test",
            model=kwargs["model"],
            choices=[choice],
            usage=usage,
            system_fingerprint=None,
        )


class FakeClient:
    def __init__(self):
        self.chat = SimpleNamespace(completions=FakeChatCompletions())
        self.responses = SimpleNamespace(create=self._responses_create)
        self.responses_calls = []

    def _responses_create(self, **kwargs):
        self.responses_calls.append(kwargs)
        usage = SimpleNamespace(input_tokens=4, output_tokens=2)
        return SimpleNamespace(
            id="resp_test",
            model=kwargs["model"],
            output_text="OK",
            usage=usage,
        )


def test_chat_completion_omits_null_max_tokens():
    client = FakeClient()
    response = openai_synch._chat_completion_create(
        client,
        {"model": "gpt-5.6-terra", "messages": [{"role": "user", "content": "Hi"}]},
    )
    assert response.id == "chatcmpl_test"
    assert client.chat.completions.calls == [{
        "model": "gpt-5.6-terra",
        "messages": [{"role": "user", "content": "Hi"}],
    }]


def test_responses_fallback_preserves_chat_completion_shape(monkeypatch):
    client = FakeClient()

    def raise_auth_error(_client, _body):
        request = httpx.Request("POST", "https://api.openai.com/v1/chat/completions")
        response = httpx.Response(401, request=request)
        raise AuthenticationError("You have insufficient permissions for this operation.", response=response, body=None)

    monkeypatch.setattr(openai_synch, "_chat_completion_create", raise_auth_error)
    body = openai_synch._create_completion_body(
        client,
        {
            "model": "gpt-5.6-sol",
            "max_tokens": 8,
            "messages": [
                {"role": "system", "content": "Extract enzyme kinetics."},
                {"role": "user", "content": "Reply OK."},
            ],
        },
    )
    assert client.responses_calls[0]["model"] == "gpt-5.6-sol"
    assert client.responses_calls[0]["instructions"] == "Extract enzyme kinetics."
    assert client.responses_calls[0]["max_output_tokens"] == 16
    assert body["choices"][0]["message"]["content"] == "OK"
    assert body["usage"]["prompt_tokens"] == 4
