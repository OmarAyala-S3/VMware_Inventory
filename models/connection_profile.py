"""
models/connection_profile.py
Modelo de perfil de conexión para gestión multi-vCenter/ESXi
"""
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional
import uuid


class ConnectionType(Enum):
    VCENTER = "vCenter"
    ESXI = "ESXi Host"


class ConnectionStatus(Enum):
    PENDING   = "Pendiente"
    TESTING   = "Probando..."
    OK        = "Conectado"
    SCANNING  = "Escaneando..."
    DONE      = "Completado"
    ERROR     = "Error"
    SKIPPED   = "Omitido"


@dataclass
class ConnectionProfile:
    """
    Representa una conexión individual a vCenter o ESXi.
    Cada perfil tiene un ID único para rastrearlo en la UI y en threads.
    """
    host: str
    username: str
    password: str
    connection_type: ConnectionType = ConnectionType.VCENTER
    port: int = 443
    ignore_ssl: bool = True
    alias: str = ""                          # Nombre amigable opcional

    # Campos de runtime (no se persisten)
    id: str = field(default_factory=lambda: str(uuid.uuid4())[:8])
    status: ConnectionStatus = field(default=ConnectionStatus.PENDING)
    error_message: str = ""
    vms_found: int = 0
    hosts_found: int = 0
    datastores_found: int = 0

    def __post_init__(self):
        if not self.alias:
            self.alias = self.host

    @property
    def display_name(self) -> str:
        return f"[{self.connection_type.value}] {self.alias}"

    @property
    def is_ready(self) -> bool:
        return self.status == ConnectionStatus.OK

    @property
    def has_error(self) -> bool:
        return self.status == ConnectionStatus.ERROR

    def reset_status(self):
        self.status = ConnectionStatus.PENDING
        self.error_message = ""
        self.vms_found = 0
        self.hosts_found = 0
        self.datastores_found = 0

    def to_dict(self) -> dict:
        """Serialización para guardar perfiles (sin contraseña)"""
        return {
            "id": self.id,
            "host": self.host,
            "username": self.username,
            "connection_type": self.connection_type.value,
            "port": self.port,
            "ignore_ssl": self.ignore_ssl,
            "alias": self.alias,
        }

    @classmethod
    def from_dict(cls, data: dict, password: str = "") -> "ConnectionProfile":
        """Deserialización desde dict guardado"""
        return cls(
            host=data["host"],
            username=data["username"],
            password=password,
            connection_type=ConnectionType(data.get("connection_type", "vCenter")),
            port=data.get("port", 443),
            ignore_ssl=data.get("ignore_ssl", True),
            alias=data.get("alias", data["host"]),
        )


@dataclass
class ScanConfig:
    """
    Configuración del modo de escaneo masivo.
    """
    parallel: bool = False               # True = paralelo, False = secuencial
    max_workers: int = 3                 # Hilos máximos en modo paralelo
    timeout: int = 30                    # Timeout por conexión (segundos)
    retry_on_error: bool = False         # Reintentar conexiones fallidas
    export_partial: bool = True          # Exportar aunque haya errores parciales
    include_vms: bool = True
    include_hosts: bool = True
    include_datastores: bool = True
    include_networks: bool = True

    @property
    def mode_label(self) -> str:
        if self.parallel:
            return f"Paralelo ({self.max_workers} workers)"
        return "Secuencial"
