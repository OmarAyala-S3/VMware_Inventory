"""
Interfaz grafica principal - ttkbootstrap
UI profesional para inventario VMware
"""
import sys
import os
import threading
import queue
import tkinter as tk
from tkinter import ttk, messagebox, filedialog
from datetime import datetime
from typing import Optional, List
from ui.multi_tab import MultiScanTab

try:
    import ttkbootstrap as tbs
    from ttkbootstrap.constants import *
    BOOTSTRAP = True
except ImportError:
    import tkinter.ttk as tbs
    BOOTSTRAP = False

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from ui.multi_tab import MultiScanTab
from services.vmware_service import VMwareService, VMwareConnectionError
from exporters.excel_exporter import ExcelExporter
from utils.credentials import (
    save_profile, load_profile, list_profiles, delete_profile, CRYPTO_AVAILABLE
)


# ── Colores del tema ───────────────────────────────────────────────────────────
CLR_BG         = "#0F1C2E"
CLR_PANEL      = "#152333"
CLR_CARD       = "#1A2D42"
CLR_BORDER     = "#1E4976"
CLR_ACCENT     = "#2196F3"
CLR_ACCENT2    = "#00BCD4"
CLR_SUCCESS    = "#4CAF50"
CLR_WARNING    = "#FF9800"
CLR_DANGER     = "#F44336"
CLR_TEXT       = "#E8F4FD"
CLR_MUTED      = "#7BA7C2"
CLR_HEADER_BG  = "#0D2B4E"


class VMwareInventoryApp:
    def __init__(self):
        if BOOTSTRAP:
            self.root = tbs.Window(themename="darkly")
        else:
            self.root = tk.Tk()

        self.root.title("VMware Inventory System")
        self.root.geometry("1280x820")
        self.root.minsize(1100, 700)
        self.root.configure(bg=CLR_BG)

        # Estado de la app
        self.service = VMwareService(log_callback=self._log)
        self.exporter = ExcelExporter(log_callback=self._log)
        self.vms = []
        self.hosts = []
        self.datastores = []
        self.networks = []
        self._log_queue = queue.Queue()
        self._working = False
        self._output_dir = os.path.expanduser("~")

        # Variables Tkinter
        self.var_conn_type  = tk.StringVar(value="vcenter")
        self.var_host       = tk.StringVar()
        self.var_port       = tk.StringVar(value="443")
        self.var_user       = tk.StringVar()
        self.var_pass       = tk.StringVar()
        self.var_ignore_ssl = tk.BooleanVar(value=True)
        self.var_save_prof  = tk.BooleanVar(value=False)
        self.var_prof_name  = tk.StringVar()
        self.var_status     = tk.StringVar(value="Sin conexion")
        self.var_progress   = tk.DoubleVar(value=0)
        self.var_progress_lbl = tk.StringVar(value="Listo")
        self.var_extract_vms  = tk.BooleanVar(value=True)
        self.var_extract_hosts = tk.BooleanVar(value=True)
        self.var_extract_ds   = tk.BooleanVar(value=True)
        self.var_extract_nets = tk.BooleanVar(value=True)
        self.var_preview_type = tk.StringVar(value="VMs")
        self.var_search       = tk.StringVar()
        self.var_search.trace_add("write", lambda *a: self._filter_preview())
        self.var_output_dir   = tk.StringVar(value=self._output_dir)

        self._build_ui()
        self._poll_log_queue()

    def _build_ui(self):
        # ── Barra de titulo ──────────────────────────────────────────────
        self._build_titlebar()

        # ── Layout principal: sidebar + contenido ────────────────────────
        main_frame = tk.Frame(self.root, bg=CLR_BG)
        main_frame.pack(fill=tk.BOTH, expand=True, padx=0, pady=0)

        self._build_sidebar(main_frame)
        self._build_content(main_frame)

    # ── BARRA DE TITULO ──────────────────────────────────────────────────────
    def _build_titlebar(self):
        bar = tk.Frame(self.root, bg=CLR_HEADER_BG, height=52)
        bar.pack(fill=tk.X, side=tk.TOP)
        bar.pack_propagate(False)

        # Logo / titulo
        tk.Label(
            bar, text="⚡  VMware Inventory System",
            font=("Segoe UI", 15, "bold"), fg=CLR_TEXT, bg=CLR_HEADER_BG,
            padx=20
        ).pack(side=tk.LEFT, pady=10)

        # Version
        tk.Label(
            bar, text="v1.0", font=("Segoe UI", 9), fg=CLR_MUTED, bg=CLR_HEADER_BG
        ).pack(side=tk.LEFT, pady=14)

        # Status
        status_frame = tk.Frame(bar, bg=CLR_HEADER_BG)
        status_frame.pack(side=tk.RIGHT, padx=20, pady=10)
        tk.Label(status_frame, text="Estado:", font=("Segoe UI", 9),
                 fg=CLR_MUTED, bg=CLR_HEADER_BG).pack(side=tk.LEFT)
        self.lbl_status = tk.Label(status_frame, textvariable=self.var_status,
                                   font=("Segoe UI", 9, "bold"), fg=CLR_DANGER,
                                   bg=CLR_HEADER_BG)
        self.lbl_status.pack(side=tk.LEFT, padx=(4, 0))

    # ── SIDEBAR ──────────────────────────────────────────────────────────────
    def _build_sidebar(self, parent):
        sb = tk.Frame(parent, bg=CLR_PANEL, width=330)
        sb.pack(side=tk.LEFT, fill=tk.Y)
        sb.pack_propagate(False)

        # Scrollable canvas para sidebar
        canvas = tk.Canvas(sb, bg=CLR_PANEL, highlightthickness=0)
        scrollbar = tk.Scrollbar(sb, orient="vertical", command=canvas.yview)
        canvas.configure(yscrollcommand=scrollbar.set)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

        inner = tk.Frame(canvas, bg=CLR_PANEL)
        window = canvas.create_window((0, 0), window=inner, anchor="nw")

        def on_frame_configure(e):
            canvas.configure(scrollregion=canvas.bbox("all"))
        inner.bind("<Configure>", on_frame_configure)

        def on_canvas_configure(e):
            canvas.itemconfig(window, width=e.width)
        canvas.bind("<Configure>", on_canvas_configure)

        # Mousewheel
        def on_mousewheel(e):
            canvas.yview_scroll(int(-1 * (e.delta / 120)), "units")
        canvas.bind_all("<MouseWheel>", on_mousewheel)

        self._build_connection_panel(inner)
        self._build_profiles_panel(inner)
        self._build_options_panel(inner)
        self._build_output_panel(inner)
        self._build_action_panel(inner)

    def _section_header(self, parent, text):
        f = tk.Frame(parent, bg=CLR_PANEL)
        f.pack(fill=tk.X, padx=12, pady=(14, 4))
        tk.Label(f, text=text, font=("Segoe UI", 10, "bold"),
                 fg=CLR_ACCENT2, bg=CLR_PANEL).pack(side=tk.LEFT)
        tk.Frame(f, bg=CLR_BORDER, height=1).pack(
            side=tk.LEFT, fill=tk.X, expand=True, padx=(8, 0), pady=8)

    def _labeled_entry(self, parent, label, var, show=None, width=28):
        f = tk.Frame(parent, bg=CLR_PANEL)
        f.pack(fill=tk.X, padx=14, pady=3)
        tk.Label(f, text=label, font=("Segoe UI", 9),
                 fg=CLR_MUTED, bg=CLR_PANEL, width=14, anchor="w").pack(side=tk.LEFT)
        e = tk.Entry(f, textvariable=var, font=("Segoe UI", 10),
                     bg=CLR_CARD, fg=CLR_TEXT, insertbackground=CLR_TEXT,
                     relief=tk.FLAT, bd=0, show=show, width=width,
                     highlightthickness=1, highlightcolor=CLR_ACCENT,
                     highlightbackground=CLR_BORDER)
        e.pack(side=tk.LEFT, fill=tk.X, expand=True, ipady=5, padx=(4, 0))
        return e

    def _build_connection_panel(self, parent):
        self._section_header(parent, "CONEXION")
        card = tk.Frame(parent, bg=CLR_CARD, relief=tk.FLAT)
        card.pack(fill=tk.X, padx=12, pady=4)
        card.configure(highlightthickness=1, highlightbackground=CLR_BORDER)

        # Tipo de conexion
        tf = tk.Frame(card, bg=CLR_CARD)
        tf.pack(fill=tk.X, padx=10, pady=(10, 6))
        tk.Label(tf, text="Tipo:", font=("Segoe UI", 9),
                 fg=CLR_MUTED, bg=CLR_CARD, width=14, anchor="w").pack(side=tk.LEFT)
        for val, text in [("vcenter", "vCenter"), ("esxi", "Host ESXi")]:
            tk.Radiobutton(
                tf, text=text, variable=self.var_conn_type, value=val,
                font=("Segoe UI", 9), fg=CLR_TEXT, bg=CLR_CARD,
                selectcolor=CLR_ACCENT, activebackground=CLR_CARD,
                activeforeground=CLR_TEXT
            ).pack(side=tk.LEFT, padx=6)

        self._labeled_entry(card, "Host/IP/FQDN:", self.var_host)
        self._labeled_entry(card, "Puerto:", self.var_port, width=8)
        self._labeled_entry(card, "Usuario:", self.var_user)
        self._labeled_entry(card, "Contrasena:", self.var_pass, show="*")

        # SSL
        ssl_f = tk.Frame(card, bg=CLR_CARD)
        ssl_f.pack(fill=tk.X, padx=14, pady=(6, 10))
        tk.Checkbutton(
            ssl_f, text="Ignorar certificado SSL",
            variable=self.var_ignore_ssl,
            font=("Segoe UI", 9), fg=CLR_MUTED, bg=CLR_CARD,
            selectcolor=CLR_CARD, activebackground=CLR_CARD,
            activeforeground=CLR_TEXT
        ).pack(side=tk.LEFT)

        # Botones conexion
        bf = tk.Frame(parent, bg=CLR_PANEL)
        bf.pack(fill=tk.X, padx=12, pady=6)
        self._btn(bf, "  Probar Conexion", self._test_connection,
                  CLR_PANEL, CLR_ACCENT).pack(side=tk.LEFT, fill=tk.X, expand=True, padx=(0, 4))
        self._btn_danger(bf, "  Desconectar", self._disconnect).pack(
            side=tk.LEFT, fill=tk.X, expand=True)

    def _build_profiles_panel(self, parent):
        self._section_header(parent, "PERFILES GUARDADOS")
        card = tk.Frame(parent, bg=CLR_CARD,
                        highlightthickness=1, highlightbackground=CLR_BORDER)
        card.pack(fill=tk.X, padx=12, pady=4)

        pf = tk.Frame(card, bg=CLR_CARD)
        pf.pack(fill=tk.X, padx=10, pady=8)
        tk.Label(pf, text="Perfil:", font=("Segoe UI", 9),
                 fg=CLR_MUTED, bg=CLR_CARD).pack(side=tk.LEFT)
        self.cmb_profiles = ttk.Combobox(
            pf, state="readonly", font=("Segoe UI", 9), width=16)
        self.cmb_profiles.pack(side=tk.LEFT, padx=6, fill=tk.X, expand=True)
        self._refresh_profiles()

        bf = tk.Frame(card, bg=CLR_CARD)
        bf.pack(fill=tk.X, padx=10, pady=(0, 8))
        self._btn(bf, "Cargar", self._load_profile,
                  CLR_CARD, CLR_SUCCESS).pack(side=tk.LEFT, padx=(0, 4))
        self._btn(bf, "Eliminar", self._delete_profile,
                  CLR_CARD, CLR_DANGER).pack(side=tk.LEFT)

        # Guardar perfil
        sf = tk.Frame(card, bg=CLR_CARD)
        sf.pack(fill=tk.X, padx=10, pady=(4, 8))
        tk.Checkbutton(
            sf, text="Guardar como:", variable=self.var_save_prof,
            font=("Segoe UI", 9), fg=CLR_MUTED, bg=CLR_CARD,
            selectcolor=CLR_CARD, activebackground=CLR_CARD, activeforeground=CLR_TEXT
        ).pack(side=tk.LEFT)
        tk.Entry(
            sf, textvariable=self.var_prof_name, font=("Segoe UI", 9),
            bg=CLR_BG, fg=CLR_TEXT, insertbackground=CLR_TEXT, relief=tk.FLAT,
            width=14, highlightthickness=1, highlightcolor=CLR_ACCENT,
            highlightbackground=CLR_BORDER
        ).pack(side=tk.LEFT, padx=4, ipady=4)

    def _build_options_panel(self, parent):
        self._section_header(parent, "ELEMENTOS A EXTRAER")
        card = tk.Frame(parent, bg=CLR_CARD,
                        highlightthickness=1, highlightbackground=CLR_BORDER)
        card.pack(fill=tk.X, padx=12, pady=4)

        for var, label, icon in [
            (self.var_extract_vms,   "Maquinas Virtuales",  "VM"),
            (self.var_extract_hosts, "Hosts ESXi",          "HOST"),
            (self.var_extract_ds,    "Datastores",          "DS"),
            (self.var_extract_nets,  "Redes",               "NET"),
        ]:
            rf = tk.Frame(card, bg=CLR_CARD)
            rf.pack(fill=tk.X, padx=12, pady=3)
            tk.Checkbutton(
                rf, text=f"  {label}", variable=var,
                font=("Segoe UI", 9), fg=CLR_TEXT, bg=CLR_CARD,
                selectcolor=CLR_ACCENT, activebackground=CLR_CARD,
                activeforeground=CLR_TEXT
            ).pack(side=tk.LEFT)

        # Padding bottom
        tk.Frame(card, bg=CLR_CARD, height=6).pack()

    def _build_output_panel(self, parent):
        self._section_header(parent, "EXPORTACION")
        card = tk.Frame(parent, bg=CLR_CARD,
                        highlightthickness=1, highlightbackground=CLR_BORDER)
        card.pack(fill=tk.X, padx=12, pady=4)

        df = tk.Frame(card, bg=CLR_CARD)
        df.pack(fill=tk.X, padx=10, pady=8)
        tk.Label(df, text="Directorio:", font=("Segoe UI", 9),
                 fg=CLR_MUTED, bg=CLR_CARD).pack(side=tk.LEFT)
        tk.Entry(
            df, textvariable=self.var_output_dir, font=("Segoe UI", 8),
            bg=CLR_BG, fg=CLR_MUTED, insertbackground=CLR_TEXT, relief=tk.FLAT,
            state="readonly", width=18,
            highlightthickness=1, highlightbackground=CLR_BORDER
        ).pack(side=tk.LEFT, padx=4, ipady=4, fill=tk.X, expand=True)
        self._btn(df, "...", self._choose_dir, CLR_CARD, CLR_ACCENT).pack(
            side=tk.LEFT, padx=(4, 0))

    def _build_action_panel(self, parent):
        self._section_header(parent, "ACCIONES")

        self._btn_accent(
            parent, "  EXTRAER INVENTARIO COMPLETO",
            self._start_extraction
        ).pack(fill=tk.X, padx=12, pady=4, ipady=6)

        self._btn(
            parent, "  Exportar a Excel",
            self._export_excel, CLR_PANEL, CLR_SUCCESS
        ).pack(fill=tk.X, padx=12, pady=2, ipady=4)

        self._btn(
            parent, "  Limpiar Datos",
            self._clear_data, CLR_PANEL, CLR_WARNING
        ).pack(fill=tk.X, padx=12, pady=2, ipady=4)

        self._btn(
            parent, "  Cancelar Operacion",
            self._cancel, CLR_PANEL, CLR_DANGER
        ).pack(fill=tk.X, padx=12, pady=2, ipady=4)

    # ── CONTENT AREA ─────────────────────────────────────────────────────────
    def _build_content(self, parent):
        content = tk.Frame(parent, bg=CLR_BG)
        content.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

        # Progreso
        self._build_progress_bar(content)

        # Notebook
        nb_frame = tk.Frame(content, bg=CLR_BG)
        nb_frame.pack(fill=tk.BOTH, expand=True, padx=8, pady=4)

        self.notebook = ttk.Notebook(nb_frame, style="TNotebook")
        self.notebook.pack(fill=tk.BOTH, expand=True)

        # Configurar estilos
        style = ttk.Style()
        style.configure("TNotebook", background=CLR_BG, borderwidth=0)
        style.configure("TNotebook.Tab", background=CLR_PANEL, foreground=CLR_MUTED,
                        font=("Segoe UI", 9, "bold"), padding=[12, 6])
        style.map("TNotebook.Tab",
                  background=[("selected", CLR_ACCENT)],
                  foreground=[("selected", "white")])

        # Crear tabs
        self.tab_vms      = self._create_tab(self.notebook, "  VMs  ")
        self.tab_hosts    = self._create_tab(self.notebook, "  Hosts  ")
        self.tab_ds       = self._create_tab(self.notebook, "  Datastores  ")
        self.tab_nets     = self._create_tab(self.notebook, "  Redes  ")
        self.tab_log      = self._create_tab(self.notebook, "  Consola  ")
        # ── PESTAÑA MULTI-CONEXIÓN (generada automáticamente) ──────────────
        self.multi_tab = MultiScanTab(
            self.notebook,
            log_callback=self._log
        )
        self.notebook.add(self.multi_tab, text="  🌐 Multi-Conexión  ")
        # ────────────────────────────────────────────────────────────────────

        # Treeviews
        self.tree_vms    = self._build_treeview(self.tab_vms)
        self.tree_hosts  = self._build_treeview(self.tab_hosts)
        self.tree_ds     = self._build_treeview(self.tab_ds)
        self.tree_nets   = self._build_treeview(self.tab_nets)
        self._build_log_tab(self.tab_log)

        # Barra de busqueda
        self._build_search_bar(content)

        # Stats bar
        self._build_stats_bar(content)

    def _create_tab(self, nb, title):
        frame = tk.Frame(nb, bg=CLR_BG)
        nb.add(frame, text=title)
        return frame

    def _build_progress_bar(self, parent):
        pf = tk.Frame(parent, bg=CLR_PANEL, height=38)
        pf.pack(fill=tk.X, padx=8, pady=(8, 0))
        pf.pack_propagate(False)

        tk.Label(pf, textvariable=self.var_progress_lbl,
                 font=("Segoe UI", 9), fg=CLR_MUTED, bg=CLR_PANEL).pack(
            side=tk.LEFT, padx=12, pady=8)

        self.progress_bar = ttk.Progressbar(
            pf, variable=self.var_progress, maximum=100, length=200,
            mode="determinate"
        )
        self.progress_bar.pack(side=tk.RIGHT, padx=12, pady=10)

    def _build_search_bar(self, parent):
        sf = tk.Frame(parent, bg=CLR_PANEL, height=34)
        sf.pack(fill=tk.X, padx=8, pady=(4, 0))
        sf.pack_propagate(False)
        tk.Label(sf, text="Buscar:", font=("Segoe UI", 9),
                 fg=CLR_MUTED, bg=CLR_PANEL).pack(side=tk.LEFT, padx=10, pady=8)
        tk.Entry(
            sf, textvariable=self.var_search, font=("Segoe UI", 9),
            bg=CLR_CARD, fg=CLR_TEXT, insertbackground=CLR_TEXT, relief=tk.FLAT,
            width=40, highlightthickness=1, highlightcolor=CLR_ACCENT,
            highlightbackground=CLR_BORDER
        ).pack(side=tk.LEFT, padx=4, ipady=3, pady=8)
        tk.Label(sf, text="(filtra la vista activa)",
                 font=("Segoe UI", 8, "italic"), fg=CLR_MUTED, bg=CLR_PANEL).pack(
            side=tk.LEFT, pady=8)

    def _build_stats_bar(self, parent):
        sf = tk.Frame(parent, bg=CLR_HEADER_BG, height=28)
        sf.pack(fill=tk.X, padx=8, pady=(4, 8))
        sf.pack_propagate(False)
        self.lbl_stats = tk.Label(sf, text="Sin datos cargados",
                                  font=("Segoe UI", 8), fg=CLR_MUTED, bg=CLR_HEADER_BG)
        self.lbl_stats.pack(side=tk.LEFT, padx=12, pady=6)

    def _build_treeview(self, parent):
        frame = tk.Frame(parent, bg=CLR_BG)
        frame.pack(fill=tk.BOTH, expand=True)

        style = ttk.Style()
        style.configure("Custom.Treeview",
                        background=CLR_CARD,
                        foreground=CLR_TEXT,
                        fieldbackground=CLR_CARD,
                        borderwidth=0,
                        font=("Segoe UI", 9),
                        rowheight=24)
        style.configure("Custom.Treeview.Heading",
                        background=CLR_HEADER_BG,
                        foreground=CLR_TEXT,
                        font=("Segoe UI", 9, "bold"),
                        relief="flat")
        style.map("Custom.Treeview",
                  background=[("selected", CLR_ACCENT)],
                  foreground=[("selected", "white")])

        tree = ttk.Treeview(frame, style="Custom.Treeview", show="headings")
        vsb = ttk.Scrollbar(frame, orient="vertical", command=tree.yview)
        hsb = ttk.Scrollbar(frame, orient="horizontal", command=tree.xview)
        tree.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)

        vsb.pack(side=tk.RIGHT, fill=tk.Y)
        hsb.pack(side=tk.BOTTOM, fill=tk.X)
        tree.pack(fill=tk.BOTH, expand=True)

        # Menu contextual
        menu = tk.Menu(tree, tearoff=0, bg=CLR_CARD, fg=CLR_TEXT,
                       activebackground=CLR_ACCENT, activeforeground="white")
        menu.add_command(label="Copiar valor",
                         command=lambda: self._copy_cell(tree))
        menu.add_command(label="Exportar seleccion",
                         command=lambda: self._export_selection(tree))
        tree.bind("<Button-3>", lambda e: self._show_context_menu(e, menu))

        # Ordenar por columna al clic
        tree.bind("<Button-1>", lambda e: self._on_tree_click(e, tree))
        tree._sort_reverse = False
        tree._sort_col = None

        return tree

    def _build_log_tab(self, parent):
        frame = tk.Frame(parent, bg=CLR_BG)
        frame.pack(fill=tk.BOTH, expand=True)

        toolbar = tk.Frame(frame, bg=CLR_PANEL, height=32)
        toolbar.pack(fill=tk.X)
        toolbar.pack_propagate(False)
        self._btn(toolbar, "Limpiar consola", self._clear_log,
                  CLR_PANEL, CLR_MUTED).pack(side=tk.LEFT, padx=8, pady=4)
        self._btn(toolbar, "Exportar log", self._export_log,
                  CLR_PANEL, CLR_ACCENT).pack(side=tk.LEFT, padx=4, pady=4)

        text_frame = tk.Frame(frame, bg=CLR_BG)
        text_frame.pack(fill=tk.BOTH, expand=True)
        vsb = tk.Scrollbar(text_frame)
        vsb.pack(side=tk.RIGHT, fill=tk.Y)

        self.log_text = tk.Text(
            text_frame, bg="#0A0F1A", fg="#7BFFB5",
            font=("Consolas", 9), wrap=tk.WORD,
            yscrollcommand=vsb.set, relief=tk.FLAT,
            state=tk.DISABLED, padx=8, pady=8
        )
        self.log_text.pack(fill=tk.BOTH, expand=True)
        vsb.config(command=self.log_text.yview)

        # Tags de color para el log
        self.log_text.tag_config("OK",      foreground="#4CAF50")
        self.log_text.tag_config("ERROR",   foreground="#F44336")
        self.log_text.tag_config("WARN",    foreground="#FF9800")
        self.log_text.tag_config("INFO",    foreground="#7BFFB5")
        self.log_text.tag_config("DEFAULT", foreground="#7BFFB5")

    # ── BOTONES ────────────────────────────────────────────────────────────
    def _btn(self, parent, text, cmd, bg, hover_color, font_sz=9):
        b = tk.Button(
            parent, text=text, command=cmd,
            font=("Segoe UI", font_sz), bg=bg, fg=CLR_MUTED,
            relief=tk.FLAT, cursor="hand2", padx=10,
            activebackground=hover_color, activeforeground="white",
            bd=0
        )
        b.bind("<Enter>", lambda e: b.configure(fg="white"))
        b.bind("<Leave>", lambda e: b.configure(fg=CLR_MUTED))
        return b

    def _btn_accent(self, parent, text, cmd):
        b = tk.Button(
            parent, text=text, command=cmd,
            font=("Segoe UI", 10, "bold"), bg=CLR_ACCENT, fg="white",
            relief=tk.FLAT, cursor="hand2", padx=10,
            activebackground="#1976D2", activeforeground="white", bd=0
        )
        return b

    def _btn_danger(self, parent, text, cmd):
        b = tk.Button(
            parent, text=text, command=cmd,
            font=("Segoe UI", 9), bg=CLR_PANEL, fg=CLR_DANGER,
            relief=tk.FLAT, cursor="hand2", padx=10,
            activebackground=CLR_DANGER, activeforeground="white", bd=0
        )
        return b

    # ── LOGGING ───────────────────────────────────────────────────────────
    def _log(self, message: str):
        self._log_queue.put(message)

    def _poll_log_queue(self):
        try:
            while True:
                msg = self._log_queue.get_nowait()
                self._write_log(msg)
        except queue.Empty:
            pass
        self.root.after(100, self._poll_log_queue)

    def _write_log(self, msg: str):
        self.log_text.configure(state=tk.NORMAL)
        ts = datetime.now().strftime("%H:%M:%S")
        line = f"[{ts}] {msg}\n"
        tag = "DEFAULT"
        if "[OK]" in msg:
            tag = "OK"
        elif "[ERROR]" in msg:
            tag = "ERROR"
        elif "[WARN]" in msg:
            tag = "WARN"
        elif "[INFO]" in msg:
            tag = "INFO"
        self.log_text.insert(tk.END, line, tag)
        self.log_text.see(tk.END)
        self.log_text.configure(state=tk.DISABLED)

    def _clear_log(self):
        self.log_text.configure(state=tk.NORMAL)
        self.log_text.delete(1.0, tk.END)
        self.log_text.configure(state=tk.DISABLED)

    def _export_log(self):
        path = filedialog.asksaveasfilename(
            defaultextension=".txt",
            filetypes=[("Text files", "*.txt"), ("All", "*.*")],
            initialfile=f"VMware_log_{datetime.now().strftime('%Y%m%d_%H%M')}.txt"
        )
        if path:
            content = self.log_text.get(1.0, tk.END)
            with open(path, "w", encoding="utf-8") as f:
                f.write(content)
            self._log(f"[OK] Log exportado: {path}")

    # ── CONEXION ──────────────────────────────────────────────────────────
    def _test_connection(self):
        if self._working:
            return
        threading.Thread(target=self._do_test_connection, daemon=True).start()

    def _do_test_connection(self):
        self._set_working(True, "Probando conexion...")
        try:
            host = self.var_host.get().strip()
            user = self.var_user.get().strip()
            pwd  = self.var_pass.get()
            port = int(self.var_port.get() or 443)
            ssl  = self.var_ignore_ssl.get()
            ctype = self.var_conn_type.get()

            if not host or not user or not pwd:
                self._log("[ERROR] Completa todos los campos de conexion.")
                return

            tmp = VMwareService(log_callback=self._log)
            tmp.connect(host, user, pwd, port, ssl, ctype)
            tmp.disconnect()
            self._log(f"[OK] Conexion exitosa a {host}")
            self.root.after(0, lambda: messagebox.showinfo(
                "Conexion OK", f"Conexion exitosa a {host}"))
        except Exception as e:
            self._log(f"[ERROR] {e}")
            self.root.after(0, lambda: messagebox.showerror("Error de conexion", str(e)))
        finally:
            self._set_working(False)

    def _disconnect(self):
        if self.service.connected:
            self.service.disconnect()
        self.var_status.set("Sin conexion")
        self.lbl_status.configure(fg=CLR_DANGER)
        self._log("[INFO] Desconectado.")

    # ── EXTRACCION ────────────────────────────────────────────────────────
    def _start_extraction(self):
        if self._working:
            messagebox.showwarning("Ocupado", "Ya hay una operacion en curso.")
            return
        host = self.var_host.get().strip()
        user = self.var_user.get().strip()
        pwd  = self.var_pass.get()
        if not host or not user or not pwd:
            messagebox.showerror("Error", "Completa host, usuario y contrasena.")
            return
        threading.Thread(target=self._do_extraction, daemon=True).start()

    def _do_extraction(self):
        self._set_working(True, "Conectando...")
        try:
            host  = self.var_host.get().strip()
            user  = self.var_user.get().strip()
            pwd   = self.var_pass.get()
            port  = int(self.var_port.get() or 443)
            ssl   = self.var_ignore_ssl.get()
            ctype = self.var_conn_type.get()

            # Guardar perfil si esta marcado
            if self.var_save_prof.get() and self.var_prof_name.get().strip():
                try:
                    save_profile(self.var_prof_name.get().strip(), host, user, pwd,
                                 port, ctype, ssl)
                    self._log("[OK] Perfil guardado.")
                    self.root.after(0, self._refresh_profiles)
                except Exception as e:
                    self._log(f"[WARN] No se pudo guardar perfil: {e}")

            self.service.connect(host, user, pwd, port, ssl, ctype)
            self.root.after(0, lambda: self.var_status.set("Conectado"))
            self.root.after(0, lambda: self.lbl_status.configure(fg=CLR_SUCCESS))

            total_steps = sum([
                self.var_extract_vms.get(),
                self.var_extract_hosts.get(),
                self.var_extract_ds.get(),
                self.var_extract_nets.get()
            ])
            step = 0

            def prog_cb(current, total, msg):
                pct = (current / total * 100) if total > 0 else 0
                self.root.after(0, lambda: self.var_progress.set(pct))
                self.root.after(0, lambda: self.var_progress_lbl.set(msg))

            if self.var_extract_vms.get():
                self._set_working(True, "Extrayendo VMs...")
                self.vms = self.service.extract_vms(progress_callback=prog_cb)
                self.root.after(0, lambda vms=self.vms: self._populate_tree(
                    self.tree_vms, [v.to_dict() for v in vms]))
                step += 1
                self._update_global_progress(step, total_steps)

            if self.var_extract_hosts.get():
                self._set_working(True, "Extrayendo Hosts...")
                self.hosts = self.service.extract_hosts(progress_callback=prog_cb)
                self.root.after(0, lambda hosts=self.hosts: self._populate_tree(
                    self.tree_hosts, [h.to_dict() for h in hosts]))
                step += 1
                self._update_global_progress(step, total_steps)

            if self.var_extract_ds.get():
                self._set_working(True, "Extrayendo Datastores...")
                self.datastores = self.service.extract_datastores(progress_callback=prog_cb)
                self.root.after(0, lambda ds=self.datastores: self._populate_tree(
                    self.tree_ds, [d.to_dict() for d in ds]))
                step += 1
                self._update_global_progress(step, total_steps)

            if self.var_extract_nets.get():
                self._set_working(True, "Extrayendo Redes...")
                self.networks = self.service.extract_networks(progress_callback=prog_cb)
                self.root.after(0, lambda nets=self.networks: self._populate_tree(
                    self.tree_nets, [n.to_dict() for n in nets]))
                step += 1
                self._update_global_progress(step, total_steps)

            self._log("[OK] Extraccion completada.")
            self.root.after(0, self._update_stats)
            self.root.after(0, lambda: self.var_progress_lbl.set("Extraccion completada"))
            self.root.after(0, lambda: self.var_progress.set(100))

        except InterruptedError:
            self._log("[WARN] Operacion cancelada.")
        except VMwareConnectionError as e:
            self._log(f"[ERROR] {e}")
            self.root.after(0, lambda: messagebox.showerror("Error de conexion", str(e)))
        except Exception as e:
            self._log(f"[ERROR] Error inesperado: {e}")
            import traceback
            self._log(traceback.format_exc())
        finally:
            self._set_working(False)

    def _update_global_progress(self, step, total):
        pct = (step / total * 100) if total > 0 else 100
        self.root.after(0, lambda: self.var_progress.set(pct))

    # ── EXPORTACION ───────────────────────────────────────────────────────
    def _export_excel(self):
        if not any([self.vms, self.hosts, self.datastores, self.networks]):
            messagebox.showwarning("Sin datos", "No hay datos para exportar.")
            return
        threading.Thread(target=self._do_export, daemon=True).start()

    def _do_export(self):
        self._set_working(True, "Exportando Excel...")
        try:
            outdir = self.var_output_dir.get() or os.path.expanduser("~")
            os.makedirs(outdir, exist_ok=True)
            vcenter = self.var_host.get().strip()
            filepath = self.exporter.export(
                outdir,
                vms=self.vms if self.var_extract_vms.get() else None,
                hosts=self.hosts if self.var_extract_hosts.get() else None,
                datastores=self.datastores if self.var_extract_ds.get() else None,
                networks=self.networks if self.var_extract_nets.get() else None,
                vcenter_name=vcenter
            )
            if filepath:
                self._log(f"[OK] Excel exportado: {filepath}")
                self.root.after(0, lambda: messagebox.showinfo(
                    "Exportacion completada",
                    f"Archivo generado:\n{filepath}"
                ))
        except Exception as e:
            self._log(f"[ERROR] Error al exportar: {e}")
            import traceback
            self._log(traceback.format_exc())
        finally:
            self._set_working(False)

    # ── TREEVIEW ──────────────────────────────────────────────────────────
    def _populate_tree(self, tree, records):
        tree.delete(*tree.get_children())
        if not records:
            return
        cols = list(records[0].keys())
        tree.configure(columns=cols)
        for col in cols:
            tree.heading(col, text=col, anchor="w")
            tree.column(col, width=max(len(col) * 8 + 20, 80), minwidth=60)

        for r in records:
            values = [str(v) if v is not None else "" for v in r.values()]
            # Colorear filas segun estado
            estado = r.get("Estado", "")
            tag = ""
            if estado == "Encendida":
                tag = "on"
            elif estado == "Apagada":
                tag = "off"
            elif estado == "Suspendida":
                tag = "susp"
            tree.insert("", tk.END, values=values, tags=(tag,))

        tree.tag_configure("on",   background="#1A3A2A", foreground="#7FFFB5")
        tree.tag_configure("off",  background="#3A1A1A", foreground="#FFB3B3")
        tree.tag_configure("susp", background="#3A3A1A", foreground="#FFECB3")

        # Guardar records para filtrado
        tree._all_records = records

    def _filter_preview(self):
        query = self.var_search.get().lower()
        tab_idx = self.notebook.index(self.notebook.select())
        trees = [self.tree_vms, self.tree_hosts, self.tree_ds, self.tree_nets]
        if tab_idx >= len(trees):
            return
        tree = trees[tab_idx]
        if not hasattr(tree, "_all_records"):
            return

        filtered = [r for r in tree._all_records
                    if not query or any(query in str(v).lower() for v in r.values())]
        tree.delete(*tree.get_children())
        for r in filtered:
            values = [str(v) if v is not None else "" for v in r.values()]
            estado = r.get("Estado", "")
            tag = "on" if estado == "Encendida" else "off" if estado == "Apagada" else "susp" if estado == "Suspendida" else ""
            tree.insert("", tk.END, values=values, tags=(tag,))

    def _on_tree_click(self, event, tree):
        region = tree.identify_region(event.x, event.y)
        if region == "heading":
            col = tree.identify_column(event.x)
            col_idx = int(col.replace("#", "")) - 1
            if not hasattr(tree, "_all_records") or not tree._all_records:
                return
            cols = list(tree._all_records[0].keys())
            if col_idx >= len(cols):
                return
            col_name = cols[col_idx]
            reverse = getattr(tree, "_sort_reverse", False)
            if getattr(tree, "_sort_col", None) == col_name:
                reverse = not reverse
            tree._sort_col = col_name
            tree._sort_reverse = reverse
            sorted_records = sorted(
                tree._all_records,
                key=lambda r: str(r.get(col_name, "")),
                reverse=reverse
            )
            tree._all_records = sorted_records
            self._populate_tree(tree, sorted_records)

    def _show_context_menu(self, event, menu):
        try:
            menu.tk_popup(event.x_root, event.y_root)
        finally:
            menu.grab_release()

    def _copy_cell(self, tree):
        sel = tree.selection()
        if sel:
            col = tree.identify_column(tree.winfo_pointerx() - tree.winfo_rootx())
            col_idx = int(col.replace("#", "")) - 1
            values = tree.item(sel[0], "values")
            if col_idx < len(values):
                self.root.clipboard_clear()
                self.root.clipboard_append(values[col_idx])

    def _export_selection(self, tree):
        sel = tree.selection()
        if not sel:
            return
        import csv
        import io
        cols = tree["columns"]
        out = io.StringIO()
        w = csv.writer(out)
        w.writerow(cols)
        for item in sel:
            w.writerow(tree.item(item, "values"))
        self.root.clipboard_clear()
        self.root.clipboard_append(out.getvalue())
        self._log(f"[INFO] {len(sel)} filas copiadas al portapapeles como CSV")

    # ── ESTADISTICAS ──────────────────────────────────────────────────────
    def _update_stats(self):
        parts = []
        if self.vms:
            on = sum(1 for v in self.vms if v.power_state == "Encendida")
            parts.append(f"VMs: {len(self.vms)} ({on} ON)")
        if self.hosts:
            parts.append(f"Hosts: {len(self.hosts)}")
        if self.datastores:
            parts.append(f"Datastores: {len(self.datastores)}")
        if self.networks:
            parts.append(f"Redes: {len(self.networks)}")
        self.lbl_stats.configure(
            text="  |  ".join(parts) if parts else "Sin datos"
        )

    # ── PERFILES ──────────────────────────────────────────────────────────
    def _refresh_profiles(self):
        profiles = list_profiles()
        self.cmb_profiles["values"] = profiles
        if profiles:
            self.cmb_profiles.set(profiles[0])

    def _load_profile(self):
        name = self.cmb_profiles.get()
        if not name:
            return
        p = load_profile(name)
        if p:
            self.var_host.set(p.get("host", ""))
            self.var_user.set(p.get("user", ""))
            self.var_pass.set(p.get("password", ""))
            self.var_port.set(str(p.get("port", 443)))
            self.var_conn_type.set(p.get("conn_type", "vcenter"))
            self.var_ignore_ssl.set(p.get("ignore_ssl", True))
            self._log(f"[OK] Perfil '{name}' cargado.")
        else:
            messagebox.showerror("Error", f"No se pudo cargar el perfil '{name}'")

    def _delete_profile(self):
        name = self.cmb_profiles.get()
        if not name:
            return
        if messagebox.askyesno("Confirmar", f"Eliminar perfil '{name}'?"):
            delete_profile(name)
            self._refresh_profiles()
            self._log(f"[INFO] Perfil '{name}' eliminado.")

    # ── UTILIDADES UI ─────────────────────────────────────────────────────
    def _clear_data(self):
        self.vms = []
        self.hosts = []
        self.datastores = []
        self.networks = []
        for tree in [self.tree_vms, self.tree_hosts, self.tree_ds, self.tree_nets]:
            tree.delete(*tree.get_children())
            tree._all_records = []
        self.var_progress.set(0)
        self.var_progress_lbl.set("Datos limpiados")
        self._update_stats()
        self._log("[INFO] Datos limpiados.")

    def _cancel(self):
        self.service.cancel()
        self._log("[WARN] Cancelacion solicitada...")

    def _choose_dir(self):
        d = filedialog.askdirectory(initialdir=self.var_output_dir.get())
        if d:
            self.var_output_dir.set(d)
            self._output_dir = d

    def _set_working(self, state: bool, msg: str = ""):
        self._working = state
        if msg:
            self.root.after(0, lambda: self.var_progress_lbl.set(msg))
        if not state:
            self.root.after(0, lambda: self.var_progress_lbl.set("Listo"))

    def run(self):
        self._log("[INFO] VMware Inventory System iniciado.")
        self._log("[INFO] Configure la conexion en el panel izquierdo.")
        self.root.protocol("WM_DELETE_WINDOW", self._on_close)
        self.root.mainloop()

    def _on_close(self):
        if self.service.connected:
            self.service.disconnect()
        self.root.destroy()
