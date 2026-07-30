from pathlib import Path

from wechatrobot.BridgeReceiptStore import BridgeReceiptStore


def test_processed_receipt_survives_reopen(tmp_path):
    path = Path(tmp_path) / "receipts.db"
    store = BridgeReceiptStore(str(path), retention_seconds=100)
    store.record_processed("msg:1")
    store.close()

    reopened = BridgeReceiptStore(str(path), retention_seconds=100)
    assert reopened.is_processed("msg:1") is True
    reopened.close()


def test_expired_receipt_is_removed(tmp_path):
    now = [100.0]
    path = Path(tmp_path) / "receipts.db"
    store = BridgeReceiptStore(
        str(path),
        retention_seconds=5,
        now_fn=lambda: now[0],
    )
    store.record_processed("msg:expired")
    now[0] = 106.0

    assert store.is_processed("msg:expired") is False
    store.close()
