"""
Servicio VMware - Conexion y extraccion via pyVmomi con PropertyCollector
"""
import ssl
import threading
from typing import Optional, Callable, List

try:
    from pyVim.connect import SmartConnect, Disconnect
    from pyVmomi import vim, vmodl
    PYVMOMI_AVAILABLE = True
except ImportError:
    PYVMOMI_AVAILABLE = False

from models.vm_model import VMModel, HostModel, DatastoreModel, NetworkModel, NicInfo, DiskInfo


class VMwareConnectionError(Exception):
    pass


class VMwareService:
    def __init__(self, log_callback: Optional[Callable] = None):
        self.service_instance = None
        self.content = None
        self.connected = False
        self.connection_type = ""
        self.host_address = ""
        self._log = log_callback or print
        self._cancel_event = threading.Event()

    def connect(self, host, user, password, port=443, ignore_ssl=True, connection_type="vcenter"):
        if not PYVMOMI_AVAILABLE:
            raise VMwareConnectionError("pyVmomi no instalado. Ejecutar: pip install pyVmomi")

        self._log(f"[INFO] Conectando a {host}:{port}...")
        ssl_context = None
        if ignore_ssl:
            ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
            ssl_context.check_hostname = False
            ssl_context.verify_mode = ssl.CERT_NONE

        try:
            self.service_instance = SmartConnect(
                host=host, user=user, pwd=password, port=port,
                sslContext=ssl_context, connectionPoolTimeout=30
            )
            self.content = self.service_instance.RetrieveContent()
            self.connected = True
            self.connection_type = connection_type
            self.host_address = host
            self._log(f"[OK] Conexion establecida con {host}")
            return True
        except Exception as e:
            if "InvalidLogin" in type(e).__name__:
                raise VMwareConnectionError("Credenciales invalidas.")
            raise VMwareConnectionError(f"Error de conexion: {str(e)}")

    def disconnect(self):
        if self.service_instance:
            try:
                Disconnect(self.service_instance)
                self._log("[INFO] Desconectado.")
            except Exception:
                pass
            finally:
                self.service_instance = None
                self.content = None
                self.connected = False

    def cancel(self):
        self._cancel_event.set()

    def _check_cancel(self):
        if self._cancel_event.is_set():
            raise InterruptedError("Operacion cancelada por el usuario.")

    def _get_all_objects(self, vim_type, properties):
        container = self.content.viewManager.CreateContainerView(
            self.content.rootFolder, [vim_type], True
        )
        filter_spec = vmodl.query.PropertyCollector.FilterSpec(
            objectSet=[vmodl.query.PropertyCollector.ObjectSpec(
                obj=container, skip=True,
                selectSet=[vmodl.query.PropertyCollector.TraversalSpec(
                    name="traverseEntities", path="view", skip=False,
                    type=vim.view.ContainerView
                )]
            )],
            propSet=[vmodl.query.PropertyCollector.PropertySpec(
                type=vim_type, pathSet=properties
            )]
        )
        result = self.content.propertyCollector.RetrieveContents([filter_spec])
        container.Destroy()
        return result

    def _prop(self, obj_props, name, default=None):
        for prop in (obj_props or []):
            if prop.name == name:
                return prop.val
        return default

    # ── VMs ──────────────────────────────────────
    def extract_vms(self, progress_callback=None):
        self._log("[INFO] Extrayendo VMs...")
        self._cancel_event.clear()

        # Pre-cargar mapa { host_moref._moId → cpuModel } en una sola query
        host_cpu_map = {}
        try:
            host_raw = self._get_all_objects(
                vim.HostSystem, ["summary.hardware.cpuModel"]
            )
            for h in host_raw:
                cpu = self._prop(h.propSet or [], "summary.hardware.cpuModel", "")
                host_cpu_map[h.obj._moId] = cpu or ""
        except Exception as e:
            self._log(f"[WARN] No se pudo cargar mapa de CPUs de hosts: {e}")

        props = [
            "name", "config.hardware.numCPU", "config.hardware.memoryMB",
            "config.guestFullName", "config.annotation", "config.version",
            "config.hardware.device", "guest.toolsStatus", "guest.toolsVersion",
            "guest.hostName", "guest.ipAddress", "guest.net",
            "guest.guestFullName", "runtime.powerState", "runtime.host",
            "summary.config.vmPathName"
        ]
        raw = self._get_all_objects(vim.VirtualMachine, props)
        total = len(raw)
        self._log(f"[INFO] {total} VMs encontradas.")
        vms = []
        for i, obj in enumerate(raw):
            self._check_cancel()
            if progress_callback:
                progress_callback(i + 1, total, f"Procesando VM {i+1}/{total}")
            vms.append(self._parse_vm(obj.propSet or [], obj.obj, host_cpu_map))
        self._log(f"[OK] {len(vms)} VMs procesadas.")
        return vms

    def _parse_vm(self, props, obj, host_cpu_map=None):
        vm = VMModel()
        vm.vcenter = self.host_address
        vm.hostname = self._prop(props, "name", "")
        vm.vcpu = self._prop(props, "config.hardware.numCPU", 0) or 0
        vm.ram_mb = self._prop(props, "config.hardware.memoryMB", 0) or 0
        vm.ram_gb = round(vm.ram_mb / 1024, 2)
        vm.description = (self._prop(props, "config.annotation", "") or "").replace("\n", " ")
        vm.hw_version = self._prop(props, "config.version", "") or ""
        vm.tools_status = str(self._prop(props, "guest.toolsStatus", "unknown") or "unknown")
        vm.tools_version = self._prop(props, "guest.toolsVersion", "") or ""
        vm.os_name = (self._prop(props, "guest.guestFullName", "") or
                      self._prop(props, "config.guestFullName", "") or "")
        vm.ip_address = self._prop(props, "guest.ipAddress", "") or ""
        power = self._prop(props, "runtime.powerState", None)
        if power:
            vm.power_state = {"poweredOn": "Encendida", "poweredOff": "Apagada",
                              "suspended": "Suspendida"}.get(str(power), str(power))

        # Host físico: nombre y procesador via mapa pre-cargado
        host_ref = self._prop(props, "runtime.host", None)
        if host_ref:
            try:
                vm.host = host_ref.name
            except Exception:
                vm.host = ""
            # Cruzar con el mapa host_moId → cpuModel
            if host_cpu_map:
                try:
                    vm.processor = host_cpu_map.get(host_ref._moId, "") or ""
                except Exception:
                    vm.processor = ""

        vmpath = self._prop(props, "summary.config.vmPathName", "") or ""
        if "[" in vmpath:
            vm.datastore = vmpath.split("[")[1].split("]")[0]

        devices = self._prop(props, "config.hardware.device", []) or []
        for device in devices:
            if isinstance(device, vim.vm.device.VirtualEthernetCard):
                nic = NicInfo()
                nic.label = (device.deviceInfo.label if device.deviceInfo else "")
                nic.mac_address = getattr(device, "macAddress", "")
                nic.connected = (device.connectable.connected if device.connectable else False)
                if hasattr(device.backing, "network") and device.backing.network:
                    try:
                        nic.network = device.backing.network.name
                    except Exception:
                        nic.network = ""
                elif hasattr(device.backing, "port"):
                    try:
                        nic.network = device.backing.port.portgroupKey or ""
                    except Exception:
                        pass
                vm.nics.append(nic)
            elif isinstance(device, vim.vm.device.VirtualDisk):
                disk = DiskInfo()
                disk.label = (device.deviceInfo.label if device.deviceInfo else "")
                disk.size_gb = round((device.capacityInKB or 0) / 1024 / 1024, 2)
                if hasattr(device.backing, "thinProvisioned"):
                    disk.thin_provisioned = device.backing.thinProvisioned or False
                if hasattr(device.backing, "datastore") and device.backing.datastore:
                    try:
                        disk.datastore = device.backing.datastore.name
                    except Exception:
                        pass
                vm.disks.append(disk)

        guest_net = self._prop(props, "guest.net", []) or []
        for i, nic in enumerate(vm.nics):
            if i < len(guest_net):
                gn = guest_net[i]
                if hasattr(gn, "ipAddress"):
                    nic.ip_addresses = [ip for ip in (gn.ipAddress or []) if ":" not in ip]
                if not nic.network and hasattr(gn, "network"):
                    nic.network = gn.network or ""
        return vm

    # ── Hosts ─────────────────────────────────────
    def extract_hosts(self, progress_callback=None):
        self._log("[INFO] Extrayendo Hosts ESXi...")
        self._cancel_event.clear()
        props = [
            "name", "config.product.version", "config.product.build",
            "hardware.cpuInfo.numCpuCores", "hardware.cpuInfo.numCpuThreads",
            "hardware.memorySize", "hardware.systemInfo.vendor",
            "hardware.systemInfo.model", "hardware.systemInfo.serialNumber",
            "summary.hardware.cpuModel", "summary.quickStats.overallMemoryUsage",
            "summary.runtime.connectionState", "config.network.vnic", "datastore", "parent"
        ]
        raw = self._get_all_objects(vim.HostSystem, props)
        total = len(raw)
        self._log(f"[INFO] {total} Hosts encontrados.")
        hosts = []
        for i, obj in enumerate(raw):
            self._check_cancel()
            if progress_callback:
                progress_callback(i + 1, total, f"Procesando Host {i+1}/{total}")
            hosts.append(self._parse_host(obj.propSet or [], obj.obj))
        self._log(f"[OK] {len(hosts)} Hosts procesados.")
        return hosts

    def _parse_host(self, props, obj):
        h = HostModel()
        h.vcenter = self.host_address
        h.name = self._prop(props, "name", "")
        h.esxi_version = self._prop(props, "config.product.version", "") or ""
        h.build = self._prop(props, "config.product.build", "") or ""
        h.cpu_model = self._prop(props, "summary.hardware.cpuModel", "") or ""
        h.cpu_cores = self._prop(props, "hardware.cpuInfo.numCpuCores", 0) or 0
        h.cpu_threads = self._prop(props, "hardware.cpuInfo.numCpuThreads", 0) or 0
        h.vendor = self._prop(props, "hardware.systemInfo.vendor", "") or ""
        h.model = self._prop(props, "hardware.systemInfo.model", "") or ""
        h.serial_number = self._prop(props, "hardware.systemInfo.serialNumber", "") or ""
        mem = self._prop(props, "hardware.memorySize", 0) or 0
        h.ram_total_gb = round(mem / (1024 ** 3), 2)
        mem_used = self._prop(props, "summary.quickStats.overallMemoryUsage", 0) or 0
        h.ram_used_gb = round(mem_used / 1024, 2)
        conn = self._prop(props, "summary.runtime.connectionState", None)
        if conn:
            h.state = {"connected": "Conectado", "disconnected": "Desconectado",
                       "notResponding": "Sin respuesta"}.get(str(conn), str(conn))
        for vnic in (self._prop(props, "config.network.vnic", []) or []):
            if hasattr(vnic, "spec") and hasattr(vnic.spec, "ip"):
                ip = vnic.spec.ip.ipAddress
                if ip and not ip.startswith("169"):
                    h.ip_address = ip
                    break
        for ds_ref in (self._prop(props, "datastore", []) or []):
            try:
                h.datastores.append(ds_ref.name)
            except Exception:
                pass
        parent = self._prop(props, "parent", None)
        if parent:
            try:
                h.cluster = parent.name if hasattr(parent, "name") else "Standalone"
            except Exception:
                pass
        return h

    # ── Datastores ────────────────────────────────
    def extract_datastores(self, progress_callback=None):
        self._log("[INFO] Extrayendo Datastores...")
        self._cancel_event.clear()
        props = ["name", "summary.type", "summary.capacity", "summary.freeSpace",
                 "summary.accessible", "host"]
        raw = self._get_all_objects(vim.Datastore, props)
        total = len(raw)
        ds_list = []
        for i, obj in enumerate(raw):
            self._check_cancel()
            if progress_callback:
                progress_callback(i + 1, total, f"DS {i+1}/{total}")
            p = obj.propSet or []
            ds = DatastoreModel()
            ds.name = self._prop(p, "name", "")
            ds.ds_type = self._prop(p, "summary.type", "") or ""
            cap = self._prop(p, "summary.capacity", 0) or 0
            free = self._prop(p, "summary.freeSpace", 0) or 0
            ds.capacity_gb = round(cap / (1024 ** 3), 2)
            ds.free_gb = round(free / (1024 ** 3), 2)
            ds.used_gb = round(ds.capacity_gb - ds.free_gb, 2)
            ds.accessible = bool(self._prop(p, "summary.accessible", True))
            for hm in (self._prop(p, "host", []) or []):
                try:
                    ds.hosts.append(hm.key.name)
                except Exception:
                    pass
            ds_list.append(ds)
        self._log(f"[OK] {len(ds_list)} Datastores procesados.")
        return ds_list

    # ── Redes ─────────────────────────────────────
    def extract_networks(self, progress_callback=None):
        self._log("[INFO] Extrayendo Redes...")
        self._cancel_event.clear()
        networks = []
        for obj in self._get_all_objects(vim.Network, ["name", "host", "vm"]):
            self._check_cancel()
            p = obj.propSet or []
            net = NetworkModel()
            net.name = self._prop(p, "name", "")
            net.net_type = "Standard"
            for h in (self._prop(p, "host", []) or []):
                try:
                    net.hosts.append(h.name)
                except Exception:
                    pass
            net.vms_count = len(self._prop(p, "vm", []) or [])
            networks.append(net)
        try:
            for obj in self._get_all_objects(vim.dvs.DistributedVirtualPortgroup,
                                             ["name", "host", "vm", "config.defaultPortConfig"]):
                self._check_cancel()
                p = obj.propSet or []
                net = NetworkModel()
                net.name = self._prop(p, "name", "")
                net.net_type = "Distributed"
                cfg = self._prop(p, "config.defaultPortConfig", None)
                if cfg and hasattr(cfg, "vlan"):
                    try:
                        net.vlan_id = str(cfg.vlan.vlanId)
                    except Exception:
                        pass
                for h in (self._prop(p, "host", []) or []):
                    try:
                        net.hosts.append(h.name)
                    except Exception:
                        pass
                net.vms_count = len(self._prop(p, "vm", []) or [])
                networks.append(net)
        except Exception:
            pass
        self._log(f"[OK] {len(networks)} Redes procesadas.")
        return networks