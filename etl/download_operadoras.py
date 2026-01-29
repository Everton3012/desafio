import requests
from pathlib import Path
from bs4 import BeautifulSoup

BASES = {
    "ativas": "https://dadosabertos.ans.gov.br/FTP/PDA/operadoras_de_plano_de_saude_ativas/",
    "canceladas": "https://dadosabertos.ans.gov.br/FTP/PDA/operadoras_de_plano_de_saude_canceladas/",
}

RAW_DIR = Path("data/raw")
RAW_DIR.mkdir(parents=True, exist_ok=True)


def get_latest_file_url(base_url: str):
    print(f"Acessando: {base_url}")

    response = requests.get(base_url, timeout=30)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")

    links = [
        a["href"]
        for a in soup.find_all("a")
        if a.get("href", "").lower().endswith((".csv", ".txt"))
    ]

    if not links:
        raise RuntimeError("Nenhum CSV/TXT encontrado.")

    links.sort(reverse=True)

    filename = links[0]
    return base_url + filename, filename


def download_file(url, filename):
    path = RAW_DIR / filename

    if path.exists():
        print(f"Já existe: {filename}")
        return

    print(f"Baixando: {filename}")

    r = requests.get(url, stream=True, timeout=60)
    r.raise_for_status()

    with open(path, "wb") as f:
        for chunk in r.iter_content(8192):
            if chunk:
                f.write(chunk)

    print(f"Salvo em: {path}")


def download_operadoras():
    for nome, base_url in BASES.items():
        print(f"\n=== OPERADORAS {nome.upper()} ===")

        try:
            url, filename = get_latest_file_url(base_url)
            download_file(url, filename)
        except Exception as e:
            print(f"Erro ao baixar {nome}: {e}")

    print("\nDownload de operadoras finalizado.")


if __name__ == "__main__":
    download_operadoras()
