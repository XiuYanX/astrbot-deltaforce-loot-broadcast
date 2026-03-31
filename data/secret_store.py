import base64
import os
import tempfile
from pathlib import Path

from astrbot.api import logger

from .runtime_paths import get_runtime_file_path

try:
    from cryptography.fernet import Fernet, InvalidToken
except ImportError:  # pragma: no cover - resolved by plugin requirements at runtime
    Fernet = None

    class InvalidToken(Exception):
        pass


SECRET_VALUE_PREFIX = "v1"
SECRET_KEY_FILENAME = "df_red_secret.key"
DPAPI_ENTROPY = b"astrbot_plugin_deltaforce_loot_broadcast"


if os.name == "nt":
    import ctypes
    from ctypes import wintypes

    class DATA_BLOB(ctypes.Structure):
        _fields_ = [
            ("cbData", wintypes.DWORD),
            ("pbData", ctypes.POINTER(ctypes.c_byte)),
        ]

    _crypt32 = ctypes.windll.crypt32
    _kernel32 = ctypes.windll.kernel32
    _CRYPTPROTECT_UI_FORBIDDEN = 0x01

    _crypt32.CryptProtectData.argtypes = [
        ctypes.POINTER(DATA_BLOB),
        wintypes.LPCWSTR,
        ctypes.POINTER(DATA_BLOB),
        wintypes.LPVOID,
        wintypes.LPVOID,
        wintypes.DWORD,
        ctypes.POINTER(DATA_BLOB),
    ]
    _crypt32.CryptProtectData.restype = wintypes.BOOL
    _crypt32.CryptUnprotectData.argtypes = [
        ctypes.POINTER(DATA_BLOB),
        ctypes.POINTER(wintypes.LPWSTR),
        ctypes.POINTER(DATA_BLOB),
        wintypes.LPVOID,
        wintypes.LPVOID,
        wintypes.DWORD,
        ctypes.POINTER(DATA_BLOB),
    ]
    _crypt32.CryptUnprotectData.restype = wintypes.BOOL
    _kernel32.LocalFree.argtypes = [wintypes.HLOCAL]
    _kernel32.LocalFree.restype = wintypes.HLOCAL


class SecretProtector:
    def __init__(self):
        self._fernet = None
        self._plaintext_fallback_logged = False
        self._legacy_plaintext_value_logged = False

    @staticmethod
    def _restrict_file_permissions(path):
        try:
            os.chmod(path, 0o600)
        except OSError:
            pass

    @classmethod
    def _write_bytes_atomic(cls, path, payload):
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        temp_fd, temp_path = tempfile.mkstemp(
            dir=str(path.parent),
            prefix=".df_red_secret_",
            suffix=".tmp",
        )
        try:
            with os.fdopen(temp_fd, "wb") as file:
                file.write(payload)
                file.flush()
                os.fsync(file.fileno())
            os.replace(temp_path, path)
            cls._restrict_file_permissions(path)
        finally:
            if os.path.exists(temp_path):
                try:
                    os.remove(temp_path)
                except OSError:
                    pass

    @staticmethod
    def _encode_payload(raw_bytes):
        return base64.urlsafe_b64encode(raw_bytes).decode("ascii")

    @staticmethod
    def _decode_payload(encoded):
        return base64.urlsafe_b64decode(encoded.encode("ascii"))

    @staticmethod
    def _build_secret_value(backend, payload):
        return f"{SECRET_VALUE_PREFIX}:{backend}:{payload}"

    @staticmethod
    def _parse_secret_value(value):
        parts = str(value or "").split(":", 2)
        if len(parts) != 3 or parts[0] != SECRET_VALUE_PREFIX:
            return "", ""
        return parts[1], parts[2]

    @staticmethod
    def _bytes_to_blob(data):
        if not data:
            return DATA_BLOB(0, None), None
        buffer = ctypes.create_string_buffer(data, len(data))
        blob = DATA_BLOB(
            len(data),
            ctypes.cast(buffer, ctypes.POINTER(ctypes.c_byte)),
        )
        return blob, buffer

    @staticmethod
    def _blob_to_bytes(blob):
        if not blob.cbData or not blob.pbData:
            return b""
        return ctypes.string_at(blob.pbData, blob.cbData)

    def _protect_with_dpapi(self, raw_bytes):
        data_blob, data_buffer = self._bytes_to_blob(raw_bytes)
        entropy_blob, entropy_buffer = self._bytes_to_blob(DPAPI_ENTROPY)
        output_blob = DATA_BLOB()

        if not _crypt32.CryptProtectData(
            ctypes.byref(data_blob),
            "astrbot_plugin_deltaforce_loot_broadcast",
            ctypes.byref(entropy_blob),
            None,
            None,
            _CRYPTPROTECT_UI_FORBIDDEN,
            ctypes.byref(output_blob),
        ):
            raise ctypes.WinError()

        try:
            return self._blob_to_bytes(output_blob)
        finally:
            _kernel32.LocalFree(ctypes.cast(output_blob.pbData, wintypes.HLOCAL))
            del data_buffer
            del entropy_buffer

    def _unprotect_with_dpapi(self, encrypted_bytes):
        data_blob, data_buffer = self._bytes_to_blob(encrypted_bytes)
        entropy_blob, entropy_buffer = self._bytes_to_blob(DPAPI_ENTROPY)
        output_blob = DATA_BLOB()
        description = wintypes.LPWSTR()

        if not _crypt32.CryptUnprotectData(
            ctypes.byref(data_blob),
            ctypes.byref(description),
            ctypes.byref(entropy_blob),
            None,
            None,
            _CRYPTPROTECT_UI_FORBIDDEN,
            ctypes.byref(output_blob),
        ):
            raise ctypes.WinError()

        try:
            return self._blob_to_bytes(output_blob)
        finally:
            if description:
                _kernel32.LocalFree(ctypes.cast(description, wintypes.HLOCAL))
            _kernel32.LocalFree(ctypes.cast(output_blob.pbData, wintypes.HLOCAL))
            del data_buffer
            del entropy_buffer

    def _log_plaintext_fallback(self, reason):
        if self._plaintext_fallback_logged:
            return
        logger.warning(
            "Secret protection fallback activated; values will be stored in plaintext "
            f"until secure storage is available: {reason}"
        )
        self._plaintext_fallback_logged = True

    def _log_legacy_plaintext_value(self):
        if self._legacy_plaintext_value_logged:
            return
        logger.warning(
            "Detected legacy plaintext secret value; returning it as-is for migration."
        )
        self._legacy_plaintext_value_logged = True

    def _reset_fernet_key(self, key_path, *, reason):
        key = Fernet.generate_key()
        self._write_bytes_atomic(key_path, key)
        self._fernet = Fernet(key)
        logger.warning(
            "Reset Fernet key for secret storage at "
            f"{key_path}: {reason}"
        )
        return self._fernet

    def _get_fernet(self):
        if self._fernet is not None:
            return self._fernet
        if Fernet is None:
            raise RuntimeError(
                "The 'cryptography' package is required to protect secrets on non-Windows platforms."
            )

        key_path = get_runtime_file_path(SECRET_KEY_FILENAME)
        if not key_path.exists():
            return self._reset_fernet_key(key_path, reason="missing key file")

        try:
            key = key_path.read_bytes()
            self._fernet = Fernet(key)
            return self._fernet
        except (OSError, TypeError, ValueError) as exc:
            corrupt_path = key_path.with_suffix(f"{key_path.suffix}.corrupt")
            try:
                if corrupt_path.exists():
                    corrupt_path.unlink()
                key_path.replace(corrupt_path)
            except OSError as move_exc:
                logger.warning(
                    "Failed to preserve invalid Fernet key before reset: "
                    f"{type(move_exc).__name__}: {move_exc}"
                )
            return self._reset_fernet_key(
                key_path,
                reason=f"invalid key data ({type(exc).__name__}: {exc})",
            )

    def protect(self, value):
        text = str(value or "")
        if not text:
            return ""

        raw_bytes = text.encode("utf-8")
        if os.name == "nt":
            try:
                encrypted = self._protect_with_dpapi(raw_bytes)
            except OSError as exc:
                self._log_plaintext_fallback(f"{type(exc).__name__}: {exc}")
                return text
            return self._build_secret_value("dpapi", self._encode_payload(encrypted))

        try:
            token = self._get_fernet().encrypt(raw_bytes).decode("ascii")
        except (OSError, RuntimeError, ValueError) as exc:
            self._log_plaintext_fallback(f"{type(exc).__name__}: {exc}")
            return text
        return self._build_secret_value("fernet", token)

    def unprotect(self, value):
        text = str(value or "")
        if not text:
            return ""

        backend, payload = self._parse_secret_value(text)
        if not backend:
            self._log_legacy_plaintext_value()
            return text

        try:
            if backend == "dpapi":
                decrypted = self._unprotect_with_dpapi(self._decode_payload(payload))
                return decrypted.decode("utf-8")
            if backend == "fernet":
                decrypted = self._get_fernet().decrypt(payload.encode("ascii"))
                return decrypted.decode("utf-8")
        except (InvalidToken, OSError, ValueError, RuntimeError) as exc:
            logger.warning(f"Failed to decrypt protected secret with backend {backend}: {type(exc).__name__}: {exc}")
            return ""

        logger.warning(f"Unsupported secret backend '{backend}', ignoring protected value.")
        return ""
