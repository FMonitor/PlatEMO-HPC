"""Regression coverage for Master-pinned Settings snapshots."""
from __future__ import annotations

import hashlib
import tempfile
import unittest
from pathlib import Path

from platemo_worker.settings import resolve_settings


class SettingsResolutionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.root = Path(self.temp.name)
        self.data = self.root / "Data"
        self.data.mkdir()
        self.setting = self.data / "SettingA.mat"
        self.setting.write_bytes(b"verified-settings")

    def tearDown(self) -> None:
        self.temp.cleanup()

    def test_master_digest_is_preserved_after_successful_local_verification(self) -> None:
        digest = hashlib.sha256(self.setting.read_bytes()).hexdigest()
        resolved = resolve_settings(self.root, {"settings_file": "SettingA.mat", "settings_sha256": digest})
        self.assertEqual(resolved["settings_sha256"], digest)

    def test_mismatched_master_digest_rejects_local_settings(self) -> None:
        with self.assertRaisesRegex(ValueError, "settings_sha256 mismatch"):
            resolve_settings(self.root, {"settings_file": "SettingA.mat", "settings_sha256": "0" * 64})


if __name__ == "__main__":
    unittest.main()
