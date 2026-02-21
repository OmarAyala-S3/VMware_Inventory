"""
ui/multi_tab.py
Pestaña/frame de integración multi-conexión dentro de la app principal.
Este módulo conecta:
  - MultiConnectionPanel  (UI de gestión)
  - ConnectionManager     (lógica de orquestación)
  - MultiSourceExporter   (exportación consolidada)
"""
import os
import tkinter as tk
from tkinter import ttk, messagebox, filedialog
from typing import List

from services.connection_manager import ConnectionManager, ConsolidatedResult
from exporters.multi_exporter import MultiSourceExporter
from models.connection_profile import ConnectionProfile
from ui.multi_connection_panel import MultiConnectionPanel


class MultiScanTab(ttk.Frame):
    """
    Frame completo para la pestaña de escaneo multi-conexión.
    Diseñado para insertarse como una pestaña en un ttk.Notebook.

    Uso:
        notebook = ttk.Notebook(root)
        tab = MultiScanTab(notebook, log_callback=my_log_fn)
        notebook.add(tab, text="🌐 Multi-Conexión")
    """

    def __init__(self, parent, log_callback=None, **kwargs):
        super().__init__(parent, **kwargs)
        self._log_cb = log_callback or (lambda msg: print(msg))
        self._manager = ConnectionManager()
        self._last_result: ConsolidatedResult = None
        self._last_profiles: List[ConnectionProfile] = []

        self._build_ui()

    def _build_ui(self):
        # Panel superior: gestión de conexiones
        self._conn_panel = MultiConnectionPanel(
            self,
            manager=self._manager,
            on_scan_complete=self._on_scan_complete,
            on_log=self._log,
            padding=5
        )
        self._conn_panel.pack(fill="both", expand=True)

        # Panel inferior: acciones post-escaneo
        action_bar = ttk.LabelFrame(self, text="Exportar Resultados", padding=8)
        action_bar.pack(fill="x", padx=5, pady=(0, 5))

        ttk.Button(
            action_bar,
            text="💾 Exportar Excel Consolidado",
            command=self._on_export,
        ).pack(side="left", padx=4)

        ttk.Button(
            action_bar,
            text="📋 Ver Resumen",
            command=self._on_show_summary,
        ).pack(side="left", padx=4)

        ttk.Button(
            action_bar,
            text="🗑 Limpiar Resultados",
            command=self._on_clear,
        ).pack(side="left", padx=4)

        self._export_status = ttk.Label(action_bar, text="", foreground="green")
        self._export_status.pack(side="left", padx=10)

    # ─────────────────────────────────────────────
    # Callbacks
    # ─────────────────────────────────────────────

    def _on_scan_complete(self, result: ConsolidatedResult, profiles: List[ConnectionProfile]):
        self._last_result   = result
        self._last_profiles = profiles

        ok  = len(result.completed_profiles)
        err = len(result.failed_profiles)

        if err == 0:
            self._export_status.config(
                text=f"✅ Escaneo completado — {ok} fuentes, {result.total_vms} VMs",
                foreground="green"
            )
        else:
            self._export_status.config(
                text=f"⚠ Completado con {err} error(s) — {ok} fuentes OK, {result.total_vms} VMs",
                foreground="orange"
            )

    def _on_export(self):
        if not self._last_result or not self._last_result.has_data:
            messagebox.showwarning(
                "Sin datos",
                "No hay datos para exportar.\nEjecuta primero un escaneo."
            )
            return

        # Preguntar directorio de destino
        output_dir = filedialog.askdirectory(title="Selecciona carpeta de destino")
        if not output_dir:
            return

        try:
            self._export_status.config(text="⏳ Exportando...", foreground="blue")
            self.update()

            exporter = MultiSourceExporter(output_dir=output_dir)
            filepath = exporter.export(
                consolidated=self._last_result,
                profiles=self._last_profiles,
            )

            self._export_status.config(
                text=f"✅ Exportado: {os.path.basename(filepath)}",
                foreground="green"
            )
            self._log(f"💾 Excel exportado: {filepath}")

            if messagebox.askyesno("Éxito", f"Archivo exportado:\n{filepath}\n\n¿Abrir carpeta?"):
                os.startfile(output_dir)

        except Exception as e:
            self._export_status.config(text=f"❌ Error al exportar: {e}", foreground="red")
            self._log(f"❌ Error exportando: {e}")
            messagebox.showerror("Error", f"No se pudo exportar:\n{e}")

    def _on_show_summary(self):
        if not self._last_result:
            messagebox.showinfo("Sin datos", "Ejecuta primero un escaneo.")
            return

        # Ventana de resumen
        win = tk.Toplevel(self)
        win.title("📊 Resumen del Escaneo")
        win.geometry("500x400")
        win.grab_set()

        text = tk.Text(win, font=("Courier", 10), wrap="none")
        text.pack(fill="both", expand=True, padx=10, pady=10)

        vsb = ttk.Scrollbar(win, command=text.yview)
        vsb.pack(side="right", fill="y")
        text.config(yscrollcommand=vsb.set)

        for line in self._last_result.summary_lines():
            text.insert("end", line + "\n")

        text.insert("end", "\nDETALLE POR FUENTE:\n")
        text.insert("end", "-" * 50 + "\n")
        for p in self._last_profiles:
            inv = self._last_result.results_by_source.get(p.id)
            if inv:
                text.insert("end",
                    f"✅ {p.display_name}\n"
                    f"   VMs: {p.vms_found}  Hosts: {p.hosts_found}  "
                    f"Datastores: {p.datastores_found}\n\n"
                )
            else:
                text.insert("end",
                    f"❌ {p.display_name}\n"
                    f"   Error: {p.error_message}\n\n"
                )

        text.config(state="disabled")
        ttk.Button(win, text="Cerrar", command=win.destroy).pack(pady=5)

    def _on_clear(self):
        if messagebox.askyesno("Confirmar", "¿Limpiar resultados del último escaneo?"):
            self._last_result   = None
            self._last_profiles = []
            self._export_status.config(text="Resultados limpiados.", foreground="gray")
            self._log("🗑 Resultados limpiados.")

    def _log(self, msg: str):
        self._log_cb(msg)


# ─────────────────────────────────────────────────────────
# Para prueba standalone (sin la app completa)
# ─────────────────────────────────────────────────────────
if __name__ == "__main__":
    root = tk.Tk()
    root.title("VMware Inventory — Multi-Conexión Test")
    root.geometry("1100x700")

    # Log simple en consola
    def log(msg):
        print(msg)

    nb = ttk.Notebook(root)
    nb.pack(fill="both", expand=True)

    tab = MultiScanTab(nb, log_callback=log)
    nb.add(tab, text="🌐 Multi-Conexión")

    root.mainloop()
