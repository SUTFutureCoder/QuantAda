import alarms.wecom_alarm as wecom_module


class _FakeResponse:
    status_code = 200
    text = "ok"

    @staticmethod
    def json():
        return {"errcode": 0, "errmsg": "ok"}


def test_wecom_push_text_sends_markdown_payload(monkeypatch):
    sent = []

    def fake_post(url, json=None, headers=None, timeout=0):
        sent.append((url, json, headers, timeout))
        return _FakeResponse()

    monkeypatch.setattr(wecom_module.config, "WECOM_WEBHOOK", "https://example.invalid/wecom")
    monkeypatch.setattr(wecom_module.requests, "post", fake_post)

    alarm = wecom_module.WeComAlarm()
    alarm.push_text("hello text")

    assert len(sent) == 1
    _, payload, _, _ = sent[0]
    assert payload["msgtype"] == "markdown"
    assert payload["markdown"]["content"] == "hello text"


def test_wecom_push_status_still_sends_payload(monkeypatch):
    sent = []

    def fake_post(url, json=None, headers=None, timeout=0):
        sent.append((url, json, headers, timeout))
        return _FakeResponse()

    monkeypatch.setattr(wecom_module.config, "WECOM_WEBHOOK", "https://example.invalid/wecom")
    monkeypatch.setattr(wecom_module.requests, "post", fake_post)

    alarm = wecom_module.WeComAlarm()
    alarm.push_status("STARTED [GM_BROKER:demo]", "detail")

    assert len(sent) == 1
    _, payload, _, _ = sent[0]
    assert payload["msgtype"] == "markdown"
    assert "系统状态: STARTED [GM_BROKER:demo]" in payload["markdown"]["content"]
