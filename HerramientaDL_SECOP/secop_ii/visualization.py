"""
Visualizador gráfico en Matplotlib.
Genera gráficos a partir de los resúmenes agregados en CSV.
"""
import logging
from pathlib import Path
import matplotlib.pyplot as plt
import pandas as pd
from secop_ii.config import RESUMENES_DIR, VISUALIZACIONES_DIR

logger = logging.getLogger("secop_ii.visualization")

class SecopVisualizer:
    def __init__(self, resumenes_dir: Path = RESUMENES_DIR, output_dir: Path = VISUALIZACIONES_DIR):
        self.resumenes_dir = resumenes_dir
        self.output_dir = output_dir

        plt.style.use("seaborn-v0_8-whitegrid" if "seaborn-v0_8-whitegrid" in plt.style.available else "default")
        plt.rcParams["font.family"] = "sans-serif"
        plt.rcParams["font.sans-serif"] = ["DejaVu Sans", "Arial"]

    def plot_all(self):
        """Genera todas las figuras en formato PNG."""
        logger.info("Generando gráficos analíticos agregados con Matplotlib...")
        self.plot_anual()
        self.plot_departamentos()
        self.plot_modalidades()
        self.plot_distribucion_valores()
        self.plot_top_entidades()
        logger.info(f"Visualizaciones generadas en {self.output_dir}")

    def plot_anual(self):
        """Gráfico 1: Evolución anual de contratos y valor total en billones."""
        f_path = self.resumenes_dir / "resumen_anual.csv"
        if not f_path.exists():
            return
        df = pd.read_csv(f_path)
        df = df[df["anio"].astype(str).str.match(r"^20\d{2}$")].sort_values("anio")
        if df.empty:
            return

        fig, ax1 = plt.subplots(figsize=(10, 5), dpi=150)

        color = "#1f77b4"
        ax1.set_xlabel("Año de Firma", fontsize=11, fontweight="bold")
        ax1.set_ylabel("Cantidad de Contratos", color=color, fontsize=11, fontweight="bold")
        ax1.bar(df["anio"].astype(str), df["cantidad_contratos"], color=color, alpha=0.7, width=0.5)
        ax1.tick_params(axis="y", labelcolor=color)

        ax2 = ax1.twinx()
        color2 = "#d62728"
        ax2.set_ylabel("Valor Total (Billones COP)", color=color2, fontsize=11, fontweight="bold")
        billones = df["valor_total_contratado"] / 1e12
        ax2.plot(df["anio"].astype(str), billones, color=color2, marker="o", linewidth=2.5)
        ax2.tick_params(axis="y", labelcolor=color2)

        plt.title("Evolución Anual de la Contratación Pública en Colombia (SECOP II)", fontsize=13, fontweight="bold", pad=15)
        fig.tight_layout()
        plt.savefig(self.output_dir / "01_evolucion_anual.png")
        plt.close()

    def plot_departamentos(self):
        """Gráfico 2: Top 12 Departamentos por cantidad de contratos."""
        f_path = self.resumenes_dir / "resumen_departamentos.csv"
        if not f_path.exists():
            return
        df = pd.read_csv(f_path).head(12)
        if df.empty:
            return

        fig, ax = plt.subplots(figsize=(10, 6), dpi=150)
        df = df.sort_values("cantidad_contratos", ascending=True)

        bars = ax.barh(df["departamento"], df["cantidad_contratos"], color="#2ca02c", alpha=0.8)
        ax.set_xlabel("Número de Contratos", fontsize=11, fontweight="bold")
        ax.set_title("Top 12 Departamentos por Cantidad de Contratos", fontsize=13, fontweight="bold", pad=15)

        for bar in bars:
            width = bar.get_width()
            ax.text(width + max(df["cantidad_contratos"]) * 0.01, bar.get_y() + bar.get_height() / 2, 
                    f"{int(width):,}", va="center", ha="left", fontsize=9)

        fig.tight_layout()
        plt.savefig(self.output_dir / "02_top_departamentos.png")
        plt.close()

    def plot_modalidades(self):
        """Gráfico 3: Distribución por modalidad de contratación."""
        f_path = self.resumenes_dir / "resumen_modalidades.csv"
        if not f_path.exists():
            return
        df = pd.read_csv(f_path).head(8)
        if df.empty:
            return

        fig, ax = plt.subplots(figsize=(10, 5), dpi=150)
        df = df.sort_values("cantidad_contratos", ascending=True)

        bars = ax.barh(df["modalidad"], df["cantidad_contratos"], color="#9467bd", alpha=0.8)
        ax.set_xlabel("Cantidad de Contratos", fontsize=11, fontweight="bold")
        ax.set_title("Distribución por Modalidad de Contratación", fontsize=13, fontweight="bold", pad=15)

        fig.tight_layout()
        plt.savefig(self.output_dir / "03_modalidades_contratacion.png")
        plt.close()

    def plot_distribucion_valores(self):
        """Gráfico 4: Distribución de contratos según rango de valor."""
        f_path = self.resumenes_dir / "distribucion_rangos_valor.csv"
        if not f_path.exists():
            return
        df = pd.read_csv(f_path)
        if df.empty:
            return

        fig, ax = plt.subplots(figsize=(10, 5), dpi=150)
        bars = ax.bar(df["rango_valor"], df["cantidad_contratos"], color="#ff7f0e", alpha=0.8)
        ax.set_ylabel("Cantidad de Contratos", fontsize=11, fontweight="bold")
        ax.set_title("Distribución de Contratos según Rango de Valor", fontsize=13, fontweight="bold", pad=15)
        plt.xticks(rotation=35, ha="right")

        for bar in bars:
            height = bar.get_height()
            ax.text(bar.get_x() + bar.get_width() / 2, height + max(df["cantidad_contratos"]) * 0.01,
                    f"{int(height):,}", ha="center", va="bottom", fontsize=8)

        fig.tight_layout()
        plt.savefig(self.output_dir / "04_distribucion_valores.png")
        plt.close()

    def plot_top_entidades(self):
        """Gráfico 5: Top 10 Entidades compradoras por presupuesto."""
        f_path = self.resumenes_dir / "top_entidades_compradoras.csv"
        if not f_path.exists():
            return
        df = pd.read_csv(f_path).head(10)
        if df.empty:
            return

        fig, ax = plt.subplots(figsize=(10, 6), dpi=150)
        df["valor_billones"] = df["valor_total"] / 1e12
        df = df.sort_values("valor_billones", ascending=True)

        nombres = [n[:40] + "..." if len(n) > 40 else n for n in df["nombre_entidad"]]

        bars = ax.barh(nombres, df["valor_billones"], color="#8c564b", alpha=0.8)
        ax.set_xlabel("Valor Total Contratado (Billones COP)", fontsize=11, fontweight="bold")
        ax.set_title("Top 10 Entidades Compradoras por Presupuesto", fontsize=13, fontweight="bold", pad=15)

        fig.tight_layout()
        plt.savefig(self.output_dir / "05_top_entidades_valor.png")
        plt.close()
