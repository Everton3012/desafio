import logging
from pathlib import Path

from etl.logging_config import setup_logging
from etl.download_ans import run as download_ans_run
from etl.download_operadoras import run as download_operadoras_run
from etl.process_files import run as process_files_run
from etl.consolidate import run as consolidate_run
from etl.validate_and_aggregate import run as validate_and_aggregate_run

logger = setup_logging("run_pipeline", "pipeline.log", logging.INFO)


def main() -> None:
    logger.info("Iniciando pipeline completo.")

    download_operadoras_run()
    download_ans_run(last_n_quarters=3)

    despesas_trimestre = process_files_run()
    consolidado = consolidate_run(Path(despesas_trimestre))
    validate_and_aggregate_run(Path(consolidado))

    logger.info("Pipeline finalizado com sucesso.")


if __name__ == "__main__":
    main()
