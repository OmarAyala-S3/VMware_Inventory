"""
utils/security.py
Gestión segura de credenciales con cifrado Fernet.
"""

import os
import json
import hashlib
import base64
from pathlib import Path
from typing import Optional

try:
    from cryptography.fernet import Fernet
    CRYPTO_AVAILABLE = True
except ImportError:
    CRYPTO_AVAILABLE = False


class CredentialManager:
    """
    Gestiona el almacenamiento seguro de perfiles de conexión.
    Usa Fernet (AES-128-CBC + HMAC-SHA256) para cifrado local.
    """

    PROFILES_FILE = Path.home() / ".vmware_inventory" / "profiles.enc"
    KEY_FILE = Path.home() / ".vmware_inventory" / ".key"

    def __init__(self):
        self._ensure_dir()
        self._key = self._load_or_create_key()

    def _ensure_dir(self):
        self.PROFILES_FILE.parent.mkdir(parents=True, exist_ok=True)

    def _load_or_create_key(self) -> Optional[bytes]:
        if not CRYPTO_AVAILABLE:
            return None
        if self.KEY_FILE.exists():
            return self.KEY_FILE.read_bytes()
        key = Fernet.generate_key()
        self.KEY_FILE.write_bytes(key)
        # Ocultar archivo en Windows
        try:
            import ctypes
            ctypes.windll.kernel32.SetFileAttributesW(str(self.KEY_FILE), 2)  # FILE_ATTRIBUTE_HIDDEN
        except Exception:
            pass
        return key

    def save_profile(self, name: str, host: str, user: str,
                     password: str, port: int = 443,
                     conn_type: str = "vcenter") -> bool:
        """Guarda un perfil de conexión cifrado."""
        try:
            profiles = self.load_profiles()
            profiles[name] = {
                "host": host,
                "user": user,
                "port": port,
                "conn_type": conn_type,
                "password_hash": hashlib.sha256(password.encode()).hexdigest(),
                "password_enc": self._encrypt(password),
            }
            self._write_profiles(profiles)
            return True
        except Exception:
            return False

    def load_profile(self, name: str) -> Optional[dict]:
        """Carga un perfil y descifra la contraseña."""
        profiles = self.load_profiles()
        if name not in profiles:
            return None
        profile = profiles[name].copy()
        if profile.get("password_enc"):
            profile["password"] = self._decrypt(profile["password_enc"])
        return profile

    def load_profiles(self) -> dict:
        """Carga todos los perfiles (sin contraseñas descifradas)."""
        if not self.PROFILES_FILE.exists():
            return {}
        try:
            data = self.PROFILES_FILE.read_bytes()
            if CRYPTO_AVAILABLE and self._key:
                f = Fernet(self._key)
                decrypted = f.decrypt(data)
                return json.loads(decrypted.decode())
            else:
                return json.loads(data.decode())
        except Exception:
            return {}

    def delete_profile(self, name: str) -> bool:
        """Elimina un perfil guardado."""
        profiles = self.load_profiles()
        if name in profiles:
            del profiles[name]
            self._write_profiles(profiles)
            return True
        return False

    def _write_profiles(self, profiles: dict):
        data = json.dumps(profiles, indent=2).encode()
        if CRYPTO_AVAILABLE and self._key:
            f = Fernet(self._key)
            encrypted = f.encrypt(data)
            self.PROFILES_FILE.write_bytes(encrypted)
        else:
            self.PROFILES_FILE.write_bytes(data)

    def _encrypt(self, text: str) -> str:
        if not CRYPTO_AVAILABLE or not self._key:
            return base64.b64encode(text.encode()).decode()
        f = Fernet(self._key)
        return f.encrypt(text.encode()).decode()

    def _decrypt(self, encrypted: str) -> str:
        if not CRYPTO_AVAILABLE or not self._key:
            return base64.b64decode(encrypted.encode()).decode()
        f = Fernet(self._key)
        return f.decrypt(encrypted.encode()).decode()

    @staticmethod
    def hash_password(password: str) -> str:
        return hashlib.sha256(password.encode()).hexdigest()
