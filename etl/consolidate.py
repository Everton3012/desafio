import pandas as pd
from pathlib import Path
import logging
import zipfile

DATA_FINAL = Path("data/final")
RAW_DIR = Path("data/raw")
LOG_DIR = Path("logs")

LOG_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler(LOG_DIR / "etl.log", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)

logger = logging.getLogger("consolidate")


def load_operadoras():
    logger.info("Carregando cadastros de operadoras (ativas + canceladas).")

    files = list(RAW_DIR.glob("Relatorio_cadop*.csv")) + list(RAW_DIR.glob("Relatorio_cadop*.txt"))

    if not files:
        logger.error("Arquivo de operadoras não encontrado em data/raw.")
        return None

    dfs = []

    for file in files:
        try:
            logger.info(f"Lendo cadastro: {file.name}")
            df = pd.read_csv(file, sep=";", encoding="latin1")
            
            # Normalizar nomes de colunas IMEDIATAMENTE após ler
            col_map = {
                "Registro_ANS": "REG_ANS",
                "REGISTRO_OPERADORA": "REG_ANS",  # Adicionar este mapeamento
                "Razao_Social": "RAZAO_SOCIAL",
                "RAZAO_SOCIAL": "RAZAO_SOCIAL",  # Já pode estar em maiúsculas
                "CNPJ": "CNPJ",
            }
            
            for col in col_map:
                if col in df.columns:
                    df.rename(columns={col: col_map[col]}, inplace=True)
            
            dfs.append(df)
        except Exception as e:
            logger.error(f"Erro ao ler {file.name}: {e}")

    if not dfs:
        logger.error("Nenhum cadastro de operadora pôde ser carregado.")
        return None

    operadoras = pd.concat(dfs, ignore_index=True)

    # Agora podemos usar REG_ANS com segurança
    if "REG_ANS" in operadoras.columns:
        operadoras = operadoras.drop_duplicates(subset=["REG_ANS"])
    else:
        logger.warning("Coluna REG_ANS não encontrada, pulando remoção de duplicatas.")
        logger.warning(f"Colunas disponíveis: {list(operadoras.columns)}")

    logger.info(f"Total de operadoras carregadas: {len(operadoras)}")

    return operadoras

def consolidate():
    logger.info("Iniciando consolidação final com dados cadastrais.")

    despesas_file = DATA_FINAL / "despesas_por_operadora_trimestre.csv"

    if not despesas_file.exists():
        logger.error("Arquivo de despesas não encontrado. Execute process_files.py primeiro.")
        return

    try:
        despesas = pd.read_csv(despesas_file, sep=";")
        logger.info(f"Registros de despesas: {len(despesas)}")
    except Exception as e:
        logger.error(f"Erro ao ler despesas: {e}")
        return

    operadoras = load_operadoras()
    if operadoras is None:
        return

    # Remover normalização duplicada - já foi feita em load_operadoras()
    
    required = {"REG_ANS", "RAZAO_SOCIAL", "CNPJ"}
    if not required.issubset(set(operadoras.columns)):
        logger.error(f"Colunas esperadas não encontradas no cadastro: {operadoras.columns}")
        return

    operadoras = operadoras[["REG_ANS", "CNPJ", "RAZAO_SOCIAL"]]

    logger.info("Realizando merge despesas x operadoras.")

    final_df = despesas.merge(operadoras, on="REG_ANS", how="left")

    missing = final_df["RAZAO_SOCIAL"].isna().sum()
    if missing > 0:
        logger.warning(f"{missing} registros sem correspondência no cadastro ANS.")

    final_df = final_df[
        ["REG_ANS", "CNPJ", "RAZAO_SOCIAL", "ano", "trimestre", "VL_SALDO_FINAL"]
    ]

    final_df = final_df.sort_values(["ano", "trimestre", "VL_SALDO_FINAL"], ascending=[True, True, False])

    output_csv = DATA_FINAL / "despesas_consolidadas_final.csv"
    final_df.to_csv(output_csv, index=False, sep=";")

    logger.info(f"Arquivo final gerado: {output_csv}")

    zip_path = DATA_FINAL / "consolidado_despesas.zip"
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as z:
        z.write(output_csv, arcname=output_csv.name)

    logger.info(f"ZIP final gerado: {zip_path}")
    logger.info("Consolidação final concluída com sucesso.")

if __name__ == "__main__":
    consolidate()
