import requests
from pathlib import Path
from datetime import datetime

BASE_URL = "https://dadosabertos.ans.gov.br/FTP/PDA/demonstracoes_contabeis"

RAW_DIR = Path("data/raw")
RAW_DIR.mkdir(parents=True, exist_ok=True)

def file_exists(url: str) -> bool:
    try:
        response = requests.head(url, timeout=10)
        return response.status_code == 200
    except requests.RequestException:
        return False

def find_last_three_quarters():
    available = []

    current_year = datetime.now().year

    for year in range(current_year, current_year - 10, -1):
        for quarter in [4, 3, 2, 1]:
            filename = f"{quarter}T{year}.zip"
            url = f"{BASE_URL}/{year}/{filename}"

            if file_exists(url):
                available.append((year, quarter, url))

                if len(available) == 3:
                    return available

    return available

def download_file(url: str, output_path: Path):
    print(f"Baixando: {url}")

    response = requests.get(url, stream=True)
    response.raise_for_status()

    with open(output_path, "wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            if chunk:
                f.write(chunk)

    print(f"Salvo em: {output_path}")

if __name__ == "__main__":
    last_three = find_last_three_quarters()

    print("\nÚltimos 3 trimestres encontrados:")
    for year, quarter, url in last_three:
        print(year, quarter, url)

    print("\nIniciando downloads...\n")

    for year, quarter, url in last_three:
        filename = f"{quarter}T{year}.zip"
        output_path = RAW_DIR / filename

        if output_path.exists():
            print(f"Arquivo já existe, pulando: {filename}")
            continue

        download_file(url, output_path)

    print("\nDownload finalizado.")
