"""
Modelos de datos: VMModel, HostModel, DatastoreModel, NetworkModel
"""
from dataclasses import dataclass, field
from typing import List

@dataclass
class NicInfo:
    label: str = ""
    mac_address: str = ""
    network: str = ""
    ip_addresses: List[str] = field(default_factory=list)
    connected: bool = False

@dataclass
class DiskInfo:
    label: str = ""
    size_gb: float = 0.0
    datastore: str = ""
    thin_provisioned: bool = False

@dataclass
class VMModel:
    vcenter: str = ""
    host: str = ""
    environment: str = ""
    hostname: str = ""
    description: str = ""
    ip_address: str = ""
    mac_address: str = ""
    processor: str = ""
    vcpu: int = 0
    ram_mb: int = 0
    ram_gb: float = 0.0
    disks: List[DiskInfo] = field(default_factory=list)
    domain: str = ""
    os_name: str = ""
    os_edition: str = ""
    power_state: str = ""
    datastore: str = ""
    network: str = ""
    tools_status: str = ""
    tools_version: str = ""
    hw_version: str = ""
    nics: List[NicInfo] = field(default_factory=list)

    def to_dict(self) -> dict:
        disks_str = " | ".join(
            [f"{d.label}: {d.size_gb:.0f}GB ({d.datastore})" for d in self.disks]
        ) if self.disks else ""
        nics_str = " | ".join(
            [f"{n.label}: {n.mac_address} [{', '.join(n.ip_addresses)}]" for n in self.nics]
        ) if self.nics else ""
        first_ip = ""
        for nic in self.nics:
            if nic.ip_addresses:
                first_ip = nic.ip_addresses[0]
                break
        first_mac = self.nics[0].mac_address if self.nics else ""
        first_net = self.nics[0].network if self.nics else ""
        first_ds = self.disks[0].datastore if self.disks else self.datastore
        return {
            "vCenter": self.vcenter,
            "Host": self.host,
            "Ambiente": self.environment,
            "Hostname": self.hostname,
            "Descripcion": self.description,
            "Direccion IP": first_ip or self.ip_address,
            "MAC": first_mac or self.mac_address,
            "Procesador": self.processor,
            "vCPU": self.vcpu,
            "RAM (GB)": round(self.ram_mb / 1024, 2) if self.ram_mb else self.ram_gb,
            "Discos Configurados": disks_str,
            "Cantidad Discos": len(self.disks),
            "Storage Total (GB)": round(sum(d.size_gb for d in self.disks), 2),
            "Dominio": self.domain,
            "Sistema Operativo": self.os_name,
            "Edicion SO": self.os_edition,
            "Estado": self.power_state,
            "Datastore Principal": first_ds,
            "Red Principal": first_net or self.network,
            "NICs Detalle": nics_str,
            "VMware Tools Status": self.tools_status,
            "VMware Tools Version": self.tools_version,
            "Version HW": self.hw_version,
        }

@dataclass
class HostModel:
    vcenter: str = ""
    name: str = ""
    ip_address: str = ""
    esxi_version: str = ""
    build: str = ""
    cpu_model: str = ""
    cpu_cores: int = 0
    cpu_threads: int = 0
    ram_total_gb: float = 0.0
    ram_used_gb: float = 0.0
    datastores: List[str] = field(default_factory=list)
    state: str = ""
    cluster: str = ""
    vendor: str = ""
    model: str = ""
    serial_number: str = ""

    def to_dict(self) -> dict:
        return {
            "vCenter": self.vcenter,
            "Nombre Host": self.name,
            "IP": self.ip_address,
            "Version ESXi": self.esxi_version,
            "Build": self.build,
            "Modelo CPU": self.cpu_model,
            "Cores CPU": self.cpu_cores,
            "Threads CPU": self.cpu_threads,
            "RAM Total (GB)": round(self.ram_total_gb, 2),
            "RAM Usada (GB)": round(self.ram_used_gb, 2),
            "RAM Libre (GB)": round(self.ram_total_gb - self.ram_used_gb, 2),
            "Datastores": " | ".join(self.datastores),
            "Estado": self.state,
            "Cluster": self.cluster,
            "Fabricante": self.vendor,
            "Modelo": self.model,
            "Numero Serie": self.serial_number,
        }

@dataclass
class DatastoreModel:
    name: str = ""
    ds_type: str = ""
    capacity_gb: float = 0.0
    free_gb: float = 0.0
    used_gb: float = 0.0
    hosts: List[str] = field(default_factory=list)
    accessible: bool = True

    def to_dict(self) -> dict:
        pct = (self.used_gb / self.capacity_gb * 100) if self.capacity_gb > 0 else 0
        return {
            "Nombre": self.name,
            "Tipo": self.ds_type,
            "Capacidad Total (GB)": round(self.capacity_gb, 2),
            "Espacio Libre (GB)": round(self.free_gb, 2),
            "Espacio Usado (GB)": round(self.used_gb, 2),
            "% Usado": round(pct, 1),
            "Hosts Asociados": " | ".join(self.hosts),
            "Accesible": "Si" if self.accessible else "No",
        }

@dataclass
class NetworkModel:
    name: str = ""
    net_type: str = ""
    vlan_id: str = ""
    hosts: List[str] = field(default_factory=list)
    switch_name: str = ""
    vms_count: int = 0

    def to_dict(self) -> dict:
        return {
            "Nombre": self.name,
            "Tipo": self.net_type,
            "VLAN": self.vlan_id,
            "Hosts": " | ".join(self.hosts),
            "Switch Asociado": self.switch_name,
            "VMs Conectadas": self.vms_count,
        }
