"""
services/connection_manager.py
Orquestador de múltiples conexiones VMware con soporte paralelo/secuencial.
"""
import threading
import queue
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Callable, Optional, Dict
from dataclasses import dataclass, field

from models.connection_profile import ConnectionProfile, ConnectionStatus, ScanConfig
from services.vmware_service import VMwareService, VMwareConnectionError

logger = logging.getLogger(__name__)


@dataclass
class SimpleInventory:
    """
    Contenedor de inventario extraído de una fuente.
    Compatible con ConsolidatedResult y MultiSourceExporter.
    """
    virtual_machines: list = field(default_factory=list)
    hosts:            list = field(default_factory=list)
    datastores:       list = field(default_factory=list)
    networks:         list = field(default_factory=list)


@dataclass
class ScanProgress:
    """Evento de progreso emitido durante el escaneo"""
    profile_id: str
    profile_name: str
    status: ConnectionStatus
    message: str
    progress_pct: float = 0.0          # 0-100 global
    vms_found: int = 0
    hosts_found: int = 0
    error: str = ""


@dataclass
class ConsolidatedResult:
    """
    Resultado consolidado de todas las conexiones escaneadas.
    Agrupa datos por fuente para exportación multi-hoja.
    """
    # Resultados por perfil: { profile_id: InventoryResult }
    results_by_source: Dict[str, object] = field(default_factory=dict)
    # Perfiles procesados (éxito y error)
    completed_profiles: List[ConnectionProfile] = field(default_factory=list)
    failed_profiles: List[ConnectionProfile] = field(default_factory=list)

    @property
    def total_vms(self) -> int:
        return sum(
            len(r.virtual_machines)
            for r in self.results_by_source.values()
            if r and hasattr(r, 'virtual_machines')
        )

    @property
    def total_hosts(self) -> int:
        return sum(
            len(r.hosts)
            for r in self.results_by_source.values()
            if r and hasattr(r, 'hosts')
        )

    @property
    def total_datastores(self) -> int:
        return sum(
            len(r.datastores)
            for r in self.results_by_source.values()
            if r and hasattr(r, 'datastores')
        )

    @property
    def has_data(self) -> bool:
        return bool(self.results_by_source)

    def summary_lines(self) -> List[str]:
        lines = [
            f"{'='*50}",
            f"  RESUMEN DE ESCANEO CONSOLIDADO",
            f"{'='*50}",
            f"  Fuentes exitosas : {len(self.completed_profiles)}",
            f"  Fuentes con error: {len(self.failed_profiles)}",
            f"  Total VMs        : {self.total_vms}",
            f"  Total Hosts      : {self.total_hosts}",
            f"  Total Datastores : {self.total_datastores}",
            f"{'='*50}",
        ]
        return lines


class ConnectionManager:
    """
    Gestiona la lista de perfiles de conexión y orquesta el escaneo
    masivo en modo paralelo o secuencial según ScanConfig.
    """

    def __init__(self):
        self.profiles: List[ConnectionProfile] = []
        self._lock = threading.Lock()
        self._stop_event = threading.Event()
        self._progress_queue: queue.Queue = queue.Queue()

    # ─────────────────────────────────────────────
    # Gestión de perfiles
    # ─────────────────────────────────────────────

    def add_profile(self, profile: ConnectionProfile):
        with self._lock:
            self.profiles.append(profile)
        logger.info(f"Perfil agregado: {profile.display_name}")

    def remove_profile(self, profile_id: str):
        with self._lock:
            self.profiles = [p for p in self.profiles if p.id != profile_id]

    def get_profile(self, profile_id: str) -> Optional[ConnectionProfile]:
        return next((p for p in self.profiles if p.id == profile_id), None)

    def clear_profiles(self):
        with self._lock:
            self.profiles.clear()

    def reset_all_statuses(self):
        for p in self.profiles:
            p.reset_status()

    # ─────────────────────────────────────────────
    # Prueba de conexión individual
    # ─────────────────────────────────────────────

    def test_connection(self, profile: ConnectionProfile) -> tuple[bool, str]:
        """
        Prueba rápida de conectividad. Retorna (ok, mensaje).
        """
        profile.status = ConnectionStatus.TESTING
        try:
            svc = VMwareService()
            svc.connect(
                host=profile.host,
                user=profile.username,
                password=profile.password,
                port=profile.port,
                ignore_ssl=profile.ignore_ssl,
                connection_type=profile.connection_type.value.lower(),
            )
            svc.disconnect()
            profile.status = ConnectionStatus.OK
            return True, f"✅ Conexión exitosa a {profile.display_name}"
        except VMwareConnectionError as e:
            profile.status = ConnectionStatus.ERROR
            profile.error_message = str(e)
            return False, f"❌ Error: {e}"
        except Exception as e:
            profile.status = ConnectionStatus.ERROR
            profile.error_message = str(e)
            return False, f"❌ Error inesperado: {e}"

    # ─────────────────────────────────────────────
    # Escaneo masivo
    # ─────────────────────────────────────────────

    def start_scan(
        self,
        config: ScanConfig,
        on_progress: Callable[[ScanProgress], None],
        on_complete: Callable[[ConsolidatedResult], None],
        profiles_override: Optional[List[ConnectionProfile]] = None,
    ):
        """
        Lanza el escaneo en un hilo separado para no bloquear la UI.
        """
        self._stop_event.clear()
        targets = profiles_override or self.profiles

        thread = threading.Thread(
            target=self._run_scan,
            args=(targets, config, on_progress, on_complete),
            daemon=True,
            name="ScanOrchestrator"
        )
        thread.start()
        return thread

    def stop_scan(self):
        self._stop_event.set()

    def _run_scan(
        self,
        profiles: List[ConnectionProfile],
        config: ScanConfig,
        on_progress: Callable,
        on_complete: Callable,
    ):
        """Lógica principal de orquestación (corre en hilo separado)."""
        result = ConsolidatedResult()
        total = len(profiles)

        def emit(profile: ConnectionProfile, msg: str, pct: float, vms=0, hosts=0, error=""):
            on_progress(ScanProgress(
                profile_id=profile.id,
                profile_name=profile.display_name,
                status=profile.status,
                message=msg,
                progress_pct=pct,
                vms_found=vms,
                hosts_found=hosts,
                error=error,
            ))

        if config.parallel and total > 1:
            self._scan_parallel(profiles, config, result, emit, total)
        else:
            self._scan_sequential(profiles, config, result, emit, total)

        on_complete(result)

    def _scan_single(
        self,
        profile: ConnectionProfile,
        config: ScanConfig,
    ):
        """
        Escanea un único perfil usando los métodos individuales de VMwareService.
        Retorna un SimpleInventory (objeto con atributos virtual_machines, hosts,
        datastores, networks) o None si falla.
        """
        if self._stop_event.is_set():
            profile.status = ConnectionStatus.SKIPPED
            return None

        try:
            profile.status = ConnectionStatus.SCANNING
            svc = VMwareService()
            svc.connect(
                host=profile.host,
                user=profile.username,
                password=profile.password,
                port=profile.port,
                ignore_ssl=profile.ignore_ssl,
                connection_type=profile.connection_type.value.lower(),
            )

            # Extraer cada categoría usando los métodos reales del servicio
            vms        = svc.extract_vms()        if config.include_vms        else []
            hosts      = svc.extract_hosts()      if config.include_hosts      else []
            datastores = svc.extract_datastores() if config.include_datastores else []
            networks   = svc.extract_networks()   if config.include_networks   else []

            svc.disconnect()

            # Empaquetar en objeto simple compatible con ConsolidatedResult
            inventory = SimpleInventory(
                virtual_machines=vms or [],
                hosts=hosts or [],
                datastores=datastores or [],
                networks=networks or [],
            )

            profile.status          = ConnectionStatus.DONE
            profile.vms_found       = len(inventory.virtual_machines)
            profile.hosts_found     = len(inventory.hosts)
            profile.datastores_found = len(inventory.datastores)

            # Inyectar campo "fuente" en cada registro
            self._tag_inventory(inventory, profile)

            return inventory

        except VMwareConnectionError as e:
            profile.status = ConnectionStatus.ERROR
            profile.error_message = str(e)
            logger.error(f"Error conectando {profile.display_name}: {e}")
            return None
        except Exception as e:
            profile.status = ConnectionStatus.ERROR
            profile.error_message = str(e)
            logger.error(f"Error inesperado en {profile.display_name}: {e}", exc_info=True)
            return None

    def _scan_sequential(self, profiles, config, result, emit, total):
        """Escaneo uno por uno."""
        for idx, profile in enumerate(profiles):
            if self._stop_event.is_set():
                break

            pct_start = (idx / total) * 100
            pct_end   = ((idx + 1) / total) * 100

            emit(profile, f"Conectando a {profile.display_name}...", pct_start)
            inventory = self._scan_single(profile, config)

            if inventory is not None:
                result.results_by_source[profile.id] = inventory
                result.completed_profiles.append(profile)
                emit(
                    profile,
                    f"✅ {profile.display_name} — {profile.vms_found} VMs, {profile.hosts_found} Hosts",
                    pct_end,
                    vms=profile.vms_found,
                    hosts=profile.hosts_found,
                )
            else:
                result.failed_profiles.append(profile)
                emit(
                    profile,
                    f"❌ {profile.display_name} — {profile.error_message}",
                    pct_end,
                    error=profile.error_message,
                )

    def _scan_parallel(self, profiles, config, result, emit, total):
        """Escaneo en paralelo con ThreadPoolExecutor."""
        futures_map = {}

        with ThreadPoolExecutor(
            max_workers=min(config.max_workers, total),
            thread_name_prefix="VMScan"
        ) as executor:
            for profile in profiles:
                emit(profile, f"Encolando {profile.display_name}...", 0)
                future = executor.submit(self._scan_single, profile, config)
                futures_map[future] = profile

            completed = 0
            for future in as_completed(futures_map):
                profile = futures_map[future]
                completed += 1
                pct = (completed / total) * 100

                try:
                    inventory = future.result()
                except Exception as e:
                    profile.status = ConnectionStatus.ERROR
                    profile.error_message = str(e)
                    inventory = None

                if inventory is not None:
                    result.results_by_source[profile.id] = inventory
                    result.completed_profiles.append(profile)
                    emit(
                        profile,
                        f"✅ {profile.display_name} — {profile.vms_found} VMs",
                        pct,
                        vms=profile.vms_found,
                        hosts=profile.hosts_found,
                    )
                else:
                    result.failed_profiles.append(profile)
                    emit(
                        profile,
                        f"❌ {profile.display_name} — {profile.error_message}",
                        pct,
                        error=profile.error_message,
                    )

    def _tag_inventory(self, inventory, profile: ConnectionProfile):
        """
        Inyecta el campo 'source_name' en cada objeto del inventario
        para que el exportador sepa de qué fuente viene cada registro.
        """
        source = profile.display_name
        if inventory is None:
            return
        for vm in getattr(inventory, 'virtual_machines', []):
            vm.source_name = source
        for host in getattr(inventory, 'hosts', []):
            host.source_name = source
        for ds in getattr(inventory, 'datastores', []):
            ds.source_name = source
        for net in getattr(inventory, 'networks', []):
            net.source_name = source
