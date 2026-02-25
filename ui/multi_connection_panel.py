"""
ui/multi_connection_panel.py
Panel de gestión de múltiples conexiones VMware.
Permite agregar, editar, eliminar y probar conexiones individuales.
"""
from tkinter import ttk, messagebox, BooleanVar, DoubleVar, IntVar, StringVar, Toplevel
import threading
from typing import Callable, Optional, List

from models.connection_profile import (
    ConnectionProfile, ConnectionType, ConnectionStatus, ScanConfig
)
from services.connection_manager import ConnectionManager, ScanProgress, ConsolidatedResult

# ─────────────────────────────────────────────────────────
# Colores de estado para la tabla
# ─────────────────────────────────────────────────────────
STATUS_COLORS = {
    ConnectionStatus.PENDING:  ("#FFFFFF", "#000000"),   # blanco / negro
    ConnectionStatus.TESTING:  ("#FFF2CC", "#7D6608"),   # amarillo / marrón
    ConnectionStatus.OK:       ("#E8F5E9", "#1B5E20"),   # verde claro / verde oscuro
    ConnectionStatus.SCANNING: ("#E3F2FD", "#0D47A1"),   # azul claro / azul oscuro
    ConnectionStatus.DONE:     ("#E8F5E9", "#1B5E20"),   # verde
    ConnectionStatus.ERROR:    ("#FFEBEE", "#B71C1C"),   # rojo claro / rojo oscuro
    ConnectionStatus.SKIPPED:  ("#F5F5F5", "#757575"),   # gris
}

class AddConnectionDialog(Toplevel):
    """Diálogo modal para agregar o editar un perfil de conexión."""

    def __init__(self, parent, profile: Optional[ConnectionProfile] = None):
        super().__init__(parent)
        self.result: Optional[ConnectionProfile] = None
        self._profile = profile

        self.title("Agregar Conexión" if profile is None else "Editar Conexión")
        self.resizable(False, False)
        self.grab_set()
        self.transient(parent)

        self._build_ui()

        if profile:
            self._populate(profile)

        # Centrar
        self.update_idletasks()
        x = parent.winfo_rootx() + (parent.winfo_width() - self.winfo_width()) // 2
        y = parent.winfo_rooty() + (parent.winfo_height() - self.winfo_height()) // 2
        self.geometry(f"+{x}+{y}")

    def _build_ui(self):
        pad = {"padx": 10, "pady": 5}
        frame = ttk.Frame(self, padding=15)
        frame.pack(fill="both", expand=True)

        # Tipo de conexión
        ttk.Label(frame, text="Tipo:").grid(row=0, column=0, sticky="w", **pad)
        self._type_var = StringVar(value=ConnectionType.VCENTER.value)
        type_combo = ttk.Combobox(
            frame, textvariable=self._type_var,
            values=[t.value for t in ConnectionType],
            state="readonly", width=20
        )
        type_combo.grid(row=0, column=1, sticky="ew", **pad)

        # Alias
        ttk.Label(frame, text="Alias/Nombre:").grid(row=1, column=0, sticky="w", **pad)
        self._alias_var = StringVar()
        ttk.Entry(frame, textvariable=self._alias_var, width=30).grid(row=1, column=1, sticky="ew", **pad)

        # Host
        ttk.Label(frame, text="IP / FQDN:").grid(row=2, column=0, sticky="w", **pad)
        self._host_var = StringVar()
        ttk.Entry(frame, textvariable=self._host_var, width=30).grid(row=2, column=1, sticky="ew", **pad)

        # Puerto
        ttk.Label(frame, text="Puerto:").grid(row=3, column=0, sticky="w", **pad)
        self._port_var = IntVar(value=443)
        ttk.Entry(frame, textvariable=self._port_var, width=10).grid(row=3, column=1, sticky="w", **pad)

        # Usuario
        ttk.Label(frame, text="Usuario:").grid(row=4, column=0, sticky="w", **pad)
        self._user_var = StringVar()
        ttk.Entry(frame, textvariable=self._user_var, width=30).grid(row=4, column=1, sticky="ew", **pad)

        # Contraseña
        ttk.Label(frame, text="Contraseña:").grid(row=5, column=0, sticky="w", **pad)
        self._pass_var = StringVar()
        ttk.Entry(frame, textvariable=self._pass_var, show="•", width=30).grid(row=5, column=1, sticky="ew", **pad)

        # Ignorar SSL
        self._ssl_var = BooleanVar(value=True)
        ttk.Checkbutton(
            frame, text="Ignorar certificado SSL",
            variable=self._ssl_var
        ).grid(row=6, column=0, columnspan=2, sticky="w", **pad)

        # Botones
        btn_frame = ttk.Frame(frame)
        btn_frame.grid(row=7, column=0, columnspan=2, pady=(10, 0))
        ttk.Button(btn_frame, text="✅ Guardar",  command=self._on_save).pack(side="left", padx=5)
        ttk.Button(btn_frame, text="❌ Cancelar", command=self.destroy).pack(side="left", padx=5)

        frame.columnconfigure(1, weight=1)

    def _populate(self, profile: ConnectionProfile):
        self._type_var.set(profile.connection_type.value)
        self._alias_var.set(profile.alias)
        self._host_var.set(profile.host)
        self._port_var.set(profile.port)
        self._user_var.set(profile.username)
        self._ssl_var.set(profile.ignore_ssl)

    def _on_save(self):
        host = self._host_var.get().strip()
        user = self._user_var.get().strip()
        pwd  = self._pass_var.get()

        if not host:
            messagebox.showwarning("Validación", "El campo IP/FQDN es obligatorio.", parent=self)
            return
        if not user:
            messagebox.showwarning("Validación", "El campo Usuario es obligatorio.", parent=self)
            return
        if not pwd and self._profile is None:
            messagebox.showwarning("Validación", "La contraseña es obligatoria.", parent=self)
            return

        conn_type = ConnectionType(self._type_var.get())

        if self._profile:
            # Edición: actualizar in-place
            self._profile.host            = host
            self._profile.username        = user
            self._profile.connection_type = conn_type
            self._profile.port            = self._port_var.get()
            self._profile.ignore_ssl      = self._ssl_var.get()
            self._profile.alias           = self._alias_var.get().strip() or host
            if pwd:
                self._profile.password = pwd
            self.result = self._profile
        else:
            self.result = ConnectionProfile(
                host            = host,
                username        = user,
                password        = pwd,
                connection_type = conn_type,
                port            = self._port_var.get(),
                ignore_ssl      = self._ssl_var.get(),
                alias           = self._alias_var.get().strip() or host,
            )

        self.destroy()

class ScanConfigDialog(Toplevel):
    """Diálogo de configuración del escaneo masivo."""

    def __init__(self, parent, current_config: ScanConfig):
        super().__init__(parent)
        self.result: Optional[ScanConfig] = None
        self._cfg = current_config

        self.title("⚙ Configuración de Escaneo")
        self.resizable(False, False)
        self.grab_set()
        self.transient(parent)

        self._build_ui()

        self.update_idletasks()
        x = parent.winfo_rootx() + (parent.winfo_width() - self.winfo_width()) // 2
        y = parent.winfo_rooty() + (parent.winfo_height() - self.winfo_height()) // 2
        self.geometry(f"+{x}+{y}")

    def _build_ui(self):
        pad = {"padx": 10, "pady": 5}
        frame = ttk.Frame(self, padding=15)
        frame.pack(fill="both", expand=True)

        ttk.Label(frame, text="Modo de Escaneo", font=("", 10, "bold")).grid(
            row=0, column=0, columnspan=2, sticky="w", pady=(0, 8)
        )

        # Paralelo / Secuencial
        self._parallel_var = BooleanVar(value=self._cfg.parallel)
        ttk.Radiobutton(
            frame, text="🔁 Secuencial (más estable)",
            variable=self._parallel_var, value=False
        ).grid(row=1, column=0, columnspan=2, sticky="w", **pad)
        ttk.Radiobutton(
            frame, text="⚡ Paralelo (más rápido)",
            variable=self._parallel_var, value=True
        ).grid(row=2, column=0, columnspan=2, sticky="w", **pad)

        # Workers
        ttk.Label(frame, text="Workers paralelos:").grid(row=3, column=0, sticky="w", **pad)
        self._workers_var = IntVar(value=self._cfg.max_workers)
        ttk.Spinbox(frame, from_=1, to=10, textvariable=self._workers_var, width=6).grid(
            row=3, column=1, sticky="w", **pad
        )

        # Timeout
        ttk.Label(frame, text="Timeout (seg):").grid(row=4, column=0, sticky="w", **pad)
        self._timeout_var = IntVar(value=self._cfg.timeout)
        ttk.Spinbox(frame, from_=5, to=120, textvariable=self._timeout_var, width=6).grid(
            row=4, column=1, sticky="w", **pad
        )

        ttk.Separator(frame).grid(row=5, column=0, columnspan=2, sticky="ew", pady=8)

        ttk.Label(frame, text="Incluir en escaneo", font=("", 10, "bold")).grid(
            row=6, column=0, columnspan=2, sticky="w"
        )

        self._inc_vms_var  = BooleanVar(value=self._cfg.include_vms)
        self._inc_host_var = BooleanVar(value=self._cfg.include_hosts)
        self._inc_ds_var   = BooleanVar(value=self._cfg.include_datastores)
        self._inc_net_var  = BooleanVar(value=self._cfg.include_networks)

        ttk.Checkbutton(frame, text="Máquinas Virtuales", variable=self._inc_vms_var).grid(
            row=7, column=0, sticky="w", **pad)
        ttk.Checkbutton(frame, text="Hosts ESXi",         variable=self._inc_host_var).grid(
            row=8, column=0, sticky="w", **pad)
        ttk.Checkbutton(frame, text="Datastores",         variable=self._inc_ds_var).grid(
            row=9, column=0, sticky="w", **pad)
        ttk.Checkbutton(frame, text="Redes",              variable=self._inc_net_var).grid(
            row=10, column=0, sticky="w", **pad)

        ttk.Separator(frame).grid(row=11, column=0, columnspan=2, sticky="ew", pady=8)

        self._partial_var = BooleanVar(value=self._cfg.export_partial)
        ttk.Checkbutton(
            frame,
            text="Exportar aunque haya fuentes con error",
            variable=self._partial_var
        ).grid(row=12, column=0, columnspan=2, sticky="w", **pad)

        btn_frame = ttk.Frame(frame)
        btn_frame.grid(row=13, column=0, columnspan=2, pady=(12, 0))
        ttk.Button(btn_frame, text="✅ Aplicar",   command=self._on_apply).pack(side="left", padx=5)
        ttk.Button(btn_frame, text="❌ Cancelar",  command=self.destroy).pack(side="left", padx=5)

    def _on_apply(self):
        self.result = ScanConfig(
            parallel           = self._parallel_var.get(),
            max_workers        = self._workers_var.get(),
            timeout            = self._timeout_var.get(),
            include_vms        = self._inc_vms_var.get(),
            include_hosts      = self._inc_host_var.get(),
            include_datastores = self._inc_ds_var.get(),
            include_networks   = self._inc_net_var.get(),
            export_partial     = self._partial_var.get(),
        )
        self.destroy()

class MultiConnectionPanel(ttk.Frame):
    """
    Panel principal de gestión multi-conexión.
    Se integra en la ventana principal de la app como un Frame.

    Callbacks:
        on_scan_complete(ConsolidatedResult, List[ConnectionProfile])
        on_log(message: str)
    """

    def __init__(
        self,
        parent,
        manager: ConnectionManager,
        on_scan_complete: Callable,
        on_log: Callable[[str], None],
        **kwargs
    ):
        super().__init__(parent, **kwargs)
        self._manager          = manager
        self._on_scan_complete = on_scan_complete
        self._on_log           = on_log
        self._scan_config      = ScanConfig()
        self._scan_thread      = None

        self._build_ui()

    def _build_ui(self):
        # ── Título
        header = ttk.Frame(self)
        header.pack(fill="x", padx=5, pady=(5, 0))

        ttk.Label(
            header,
            text="🌐 Gestión de Conexiones VMware",
            font=("", 12, "bold")
        ).pack(side="left")

        ttk.Label(
            header,
            text="Agrega múltiples vCenter/ESXi para escaneo masivo",
            foreground="gray"
        ).pack(side="left", padx=10)

        # ── Barra de botones
        btn_bar = ttk.Frame(self)
        btn_bar.pack(fill="x", padx=5, pady=5)

        ttk.Button(btn_bar, text="➕ Agregar",       command=self._on_add).pack(side="left", padx=2)
        ttk.Button(btn_bar, text="✏ Editar",         command=self._on_edit).pack(side="left", padx=2)
        ttk.Button(btn_bar, text="🗑 Eliminar",       command=self._on_remove).pack(side="left", padx=2)
        ttk.Button(btn_bar, text="🔌 Probar",         command=self._on_test).pack(side="left", padx=2)

        ttk.Separator(btn_bar, orient="vertical").pack(side="left", fill="y", padx=8)

        ttk.Button(btn_bar, text="⚙ Configurar Escaneo", command=self._on_config).pack(side="left", padx=2)

        self._config_label = ttk.Label(btn_bar, text=f"Modo: {self._scan_config.mode_label}", foreground="#555")
        self._config_label.pack(side="left", padx=5)

        ttk.Separator(btn_bar, orient="vertical").pack(side="left", fill="y", padx=8)

        self._scan_btn = ttk.Button(
            btn_bar, text="🚀 Escanear Todo",
            command=self._on_scan,
            style="Accent.TButton"
        )
        self._scan_btn.pack(side="left", padx=2)

        self._stop_btn = ttk.Button(
            btn_bar, text="⏹ Detener",
            command=self._on_stop,
            state="disabled"
        )
        self._stop_btn.pack(side="left", padx=2)

        # ── Tabla de conexiones
        table_frame = ttk.LabelFrame(self, text="Conexiones Registradas", padding=5)
        table_frame.pack(fill="both", expand=True, padx=5, pady=5)

        columns = ("alias", "type", "host", "port", "user", "status", "vms", "hosts", "error")
        self._tree = ttk.Treeview(
            table_frame,
            columns=columns,
            show="headings",
            height=10,
            selectmode="browse"
        )

        col_defs = [
            ("alias",  "Alias/Nombre",   140),
            ("type",   "Tipo",            90),
            ("host",   "IP / FQDN",      150),
            ("port",   "Puerto",           55),
            ("user",   "Usuario",         100),
            ("status", "Estado",          110),
            ("vms",    "VMs",              50),
            ("hosts",  "Hosts",            50),
            ("error",  "Último Error",    200),
        ]

        for col_id, heading, width in col_defs:
            self._tree.heading(col_id, text=heading, anchor="w")
            self._tree.column(col_id, width=width, anchor="w")

        # Scrollbar
        vsb = ttk.Scrollbar(table_frame, orient="vertical",   command=self._tree.yview)
        hsb = ttk.Scrollbar(table_frame, orient="horizontal",  command=self._tree.xview)
        self._tree.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)

        self._tree.grid(row=0, column=0, sticky="nsew")
        vsb.grid(row=0, column=1, sticky="ns")
        hsb.grid(row=1, column=0, sticky="ew")
        table_frame.rowconfigure(0, weight=1)
        table_frame.columnconfigure(0, weight=1)

        # Tag styles
        self._tree.tag_configure("ok",       background="#E8F5E9", foreground="#1B5E20")
        self._tree.tag_configure("error",    background="#FFEBEE", foreground="#B71C1C")
        self._tree.tag_configure("scanning", background="#E3F2FD", foreground="#0D47A1")
        self._tree.tag_configure("done",     background="#E8F5E9", foreground="#1B5E20")
        self._tree.tag_configure("pending",  background="#FFFFFF", foreground="#000000")
        self._tree.tag_configure("testing",  background="#FFF9C4", foreground="#7D6608")

        # Double-click edita
        self._tree.bind("<Double-1>", lambda e: self._on_edit())

        # ── Barra progreso
        prog_frame = ttk.Frame(self)
        prog_frame.pack(fill="x", padx=5, pady=(0, 5))

        self._progress_var = DoubleVar(value=0)
        self._progress_bar = ttk.Progressbar(
            prog_frame,
            variable=self._progress_var,
            maximum=100,
            length=400
        )
        self._progress_bar.pack(side="left", fill="x", expand=True)

        self._progress_label = ttk.Label(prog_frame, text="0%", width=5)
        self._progress_label.pack(side="left", padx=5)

        # ── Contador resumen
        self._summary_var = StringVar(value="Sin datos")
        ttk.Label(self, textvariable=self._summary_var, foreground="gray").pack(
            anchor="e", padx=10, pady=(0, 5)
        )

    # ─────────────────────────────────────────────
    # Handlers de botones
    # ─────────────────────────────────────────────

    def _on_add(self):
        dlg = AddConnectionDialog(self)
        self.wait_window(dlg)
        if dlg.result:
            self._manager.add_profile(dlg.result)
            self._refresh_table()
            self._on_log(f"➕ Conexión agregada: {dlg.result.display_name}")

    def _on_edit(self):
        profile = self._get_selected_profile()
        if not profile:
            messagebox.showinfo("Selección", "Selecciona una conexión para editar.")
            return
        dlg = AddConnectionDialog(self, profile=profile)
        self.wait_window(dlg)
        if dlg.result:
            self._refresh_table()
            self._on_log(f"✏ Conexión actualizada: {dlg.result.display_name}")

    def _on_remove(self):
        profile = self._get_selected_profile()
        if not profile:
            messagebox.showinfo("Selección", "Selecciona una conexión para eliminar.")
            return
        if messagebox.askyesno("Confirmar", f"¿Eliminar '{profile.display_name}'?"):
            self._manager.remove_profile(profile.id)
            self._refresh_table()
            self._on_log(f"🗑 Conexión eliminada: {profile.display_name}")

    def _on_test(self):
        profile = self._get_selected_profile()
        if not profile:
            messagebox.showinfo("Selección", "Selecciona una conexión para probar.")
            return
        self._on_log(f"🔌 Probando conexión: {profile.display_name}...")
        profile.status = ConnectionStatus.TESTING
        self._refresh_table()

        def test_thread():
            ok, msg = self._manager.test_connection(profile)
            self.after(0, lambda: self._on_log(msg))
            self.after(0, self._refresh_table)

        threading.Thread(target=test_thread, daemon=True).start()

    def _on_config(self):
        dlg = ScanConfigDialog(self, self._scan_config)
        self.wait_window(dlg)
        if dlg.result:
            self._scan_config = dlg.result
            self._config_label.config(text=f"Modo: {self._scan_config.mode_label}")
            self._on_log(f"⚙ Configuración de escaneo actualizada: {self._scan_config.mode_label}")

    def _on_scan(self):
        if not self._manager.profiles:
            messagebox.showwarning("Sin conexiones", "Agrega al menos una conexión antes de escanear.")
            return

        self._manager.reset_all_statuses()
        self._refresh_table()
        self._progress_var.set(0)
        self._scan_btn.config(state="disabled")
        self._stop_btn.config(state="normal")
        self._summary_var.set("Escaneando...")
        self._on_log(f"🚀 Iniciando escaneo masivo ({len(self._manager.profiles)} fuentes) — Modo: {self._scan_config.mode_label}")

        self._scan_thread = self._manager.start_scan(
            config=self._scan_config,
            on_progress=self._on_progress,
            on_complete=self._on_complete,
        )

    def _on_stop(self):
        self._manager.stop_scan()
        self._on_log("⏹ Escaneo detenido por el usuario.")
        self._stop_btn.config(state="disabled")

    # ─────────────────────────────────────────────
    # Callbacks de escaneo (vienen desde hilos)
    # ─────────────────────────────────────────────

    def _on_progress(self, prog: ScanProgress):
        """Llamado desde hilo de escaneo — usa after() para UI thread."""
        def update():
            self._progress_var.set(prog.progress_pct)
            self._progress_label.config(text=f"{prog.progress_pct:.0f}%")
            self._on_log(f"  [{prog.profile_name}] {prog.message}")
            self._refresh_table()
        self.after(0, update)

    def _on_complete(self, result: ConsolidatedResult):
        """Llamado cuando terminan todos los escaneos."""
        def finalize():
            self._scan_btn.config(state="normal")
            self._stop_btn.config(state="disabled")
            self._progress_var.set(100)
            self._progress_label.config(text="100%")
            self._refresh_table()

            summary = result.summary_lines()
            for line in summary:
                self._on_log(line)

            self._summary_var.set(
                f"Total: {result.total_vms} VMs  |  "
                f"{result.total_hosts} Hosts  |  "
                f"{result.total_datastores} Datastores  |  "
                f"{len(result.completed_profiles)}/{len(result.completed_profiles)+len(result.failed_profiles)} fuentes OK"
            )

            # Notificar a la app principal
            self._on_scan_complete(result, self._manager.profiles)

        self.after(0, finalize)

    # ─────────────────────────────────────────────
    # Utilidades
    # ─────────────────────────────────────────────

    def _refresh_table(self):
        """Reconstruye las filas del Treeview desde los perfiles actuales."""
        selected_id = None
        sel = self._tree.selection()
        if sel:
            selected_id = self._tree.item(sel[0], "values")[0] if sel else None

        self._tree.delete(*self._tree.get_children())

        for p in self._manager.profiles:
            status_text = p.status.value
            tag = {
                ConnectionStatus.OK:       "ok",
                ConnectionStatus.DONE:     "done",
                ConnectionStatus.ERROR:    "error",
                ConnectionStatus.SCANNING: "scanning",
                ConnectionStatus.TESTING:  "testing",
            }.get(p.status, "pending")

            self._tree.insert(
                "",
                "end",
                iid=p.id,
                values=(
                    p.alias,
                    p.connection_type.value,
                    p.host,
                    p.port,
                    p.username,
                    status_text,
                    p.vms_found   if p.vms_found   else "-",
                    p.hosts_found if p.hosts_found else "-",
                    p.error_message[:60] if p.error_message else "",
                ),
                tags=(tag,)
            )

        # Restaurar selección
        if selected_id and self._tree.exists(selected_id):
            self._tree.selection_set(selected_id)

    def _get_selected_profile(self) -> Optional[ConnectionProfile]:
        sel = self._tree.selection()
        if not sel:
            return None
        profile_id = sel[0]  # El iid ES el profile.id
        return self._manager.get_profile(profile_id)

    def get_profiles(self) -> List[ConnectionProfile]:
        return self._manager.profiles
