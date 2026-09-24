"""
Visor interactivo en terminal para consultar los datos Parquet con DuckDB.
"""
import sys
from pathlib import Path
import duckdb

if sys.stdout and hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

DATA_DIR = Path(__file__).resolve().parent / "data" / "parquet"
UNIFIED_PARQUET = DATA_DIR / "SECOP_UNIFICADO_2015_2025.parquet"
SECOP_II_PARQUET = DATA_DIR / "SECOP_II_2015_2025.parquet"

target_file = UNIFIED_PARQUET if UNIFIED_PARQUET.exists() else SECOP_II_PARQUET

if not target_file.exists():
    print(f"No se encontró archivo Parquet en {DATA_DIR}. Ejecuta primero una descarga.")
    sys.exit(1)

con = duckdb.connect()
clean_path = str(target_file).replace("\\", "/")

def menu():
    while True:
        print("\n" + "=" * 60)
        print("    EXPLORADOR RÁPIDO DE DATOS PARQUET (DUCKDB)")
        print(f"    Archivo activo: {target_file.name}")
        print("=" * 60)
        print("1. Ver las primeras 5 filas (Vista previa tipo tabla)")
        print("2. Ver total de contratos y monto total en COP")
        print("3. Ver resumen de contratos por Año")
        print("4. Ver Top 10 Departamentos con más contratación")
        print("5. Ver Top 10 Proveedores con más dinero adjudicado")
        print("6. Escribir tu propia consulta SQL personalizada")
        print("7. Salir")
        print("=" * 60)
        
        opc = input("Selecciona una opción (1-7): ").strip()

        # 1. Vista previa
        if opc == "1":
            sql = f"""
            SELECT 
                id_contrato_global, 
                sistema_origen, 
                nombre_entidad, 
                proveedor, 
                valor_contrato, 
                anio_contratacion 
            FROM read_parquet('{clean_path}') 
            LIMIT 5
            """
            df = con.execute(sql).df()
            print("\n--- PRIMERAS 5 FILAS ---")
            print(df.to_string())

        # 2. Totales
        elif opc == "2":
            sql = f"""
            SELECT 
                count(*) as total_contratos,
                round(sum(valor_contrato)/1e12, 3) as total_billones_cop,
                round(avg(valor_contrato)/1e6, 2) as promedio_millones_cop,
                round(median(valor_contrato)/1e6, 2) as mediana_millones_cop
            FROM read_parquet('{clean_path}')
            """
            df = con.execute(sql).df()
            print("\n--- TOTALES Y MÉTRICAS GENERALES ---")
            print(df.to_string(index=False))

        # 3. Anual
        elif opc == "3":
            sql = f"""
            SELECT 
                anio_contratacion as anio,
                count(*) as contratos,
                round(sum(valor_contrato)/1e9, 2) as miles_de_millones_cop
            FROM read_parquet('{clean_path}')
            WHERE anio_contratacion IS NOT NULL
            GROUP BY 1 ORDER BY 1 ASC
            """
            df = con.execute(sql).df()
            print("\n--- CONTRATACIÓN POR AÑO ---")
            print(df.to_string(index=False))

        # 4. Departamentos
        elif opc == "4":
            sql = f"""
            SELECT 
                departamento,
                count(*) as contratos,
                round(sum(valor_contrato)/1e9, 2) as miles_de_millones_cop
            FROM read_parquet('{clean_path}')
            GROUP BY 1 ORDER BY contratos DESC LIMIT 10
            """
            df = con.execute(sql).df()
            print("\n--- TOP 10 DEPARTAMENTOS ---")
            print(df.to_string(index=False))

        # 5. Proveedores
        elif opc == "5":
            sql = f"""
            SELECT 
                proveedor,
                count(*) as contratos,
                round(sum(valor_contrato)/1e9, 2) as total_adjudicado_miles_millones
            FROM read_parquet('{clean_path}')
            WHERE proveedor IS NOT NULL
            GROUP BY 1 ORDER BY total_adjudicado_miles_millones DESC LIMIT 10
            """
            df = con.execute(sql).df()
            print("\n--- TOP 10 PROVEEDORES ---")
            print(df.to_string(index=False))

        # 6. SQL libre
        elif opc == "6":
            custom_sql = input("\nEscribe tu consulta SQL (Usa 'datos' como nombre de tabla):\nEjemplo: SELECT * FROM datos WHERE valor_contrato > 50000000 LIMIT 10\n> ")
            if custom_sql.strip():
                try:
                    q = custom_sql.replace("datos", f"read_parquet('{clean_path}')")
                    df = con.execute(q).df()
                    print("\n--- RESULTADO DE TU CONSULTA ---")
                    print(df.to_string())
                except Exception as e:
                    print(f"Error en la consulta: {e}")

        # 7. Salir
        elif opc == "7":
            print("Saliendo del explorador.")
            break
        else:
            print("Opción no válida.")

if __name__ == "__main__":
    menu()
