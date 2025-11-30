import json
import os
import time
from pathlib import Path
from typing import Optional

import requests
from requests.exceptions import RequestException
from tqdm import tqdm

GROBID_URL = os.environ.get("GROBID_URL", "http://localhost:8070")
# Optional throttle between Grobid calls (in seconds)
GROBID_SLEEP = float(os.environ.get("GROBID_SLEEP", "0.2"))
THIS_DIR = Path(__file__).resolve().parent
# Treat the directory containing this script as the project root
REPO_ROOT = THIS_DIR

OUTPUT_ROOT = REPO_ROOT / "pdfs_parsed"

def grobid_process_fulltext(
    pdf_path: Path,
    output_xml_path: Path,
    *,
    retries: int = 3,
    backoff: float = 5.0,
) -> None:
    """
    Send the PDF to Grobid's processFulltextDocument endpoint and save TEI XML.

    Assumes a Grobid server is running and accessible at GROBID_URL.
    """
    url = f"{GROBID_URL.rstrip('/')}/api/processFulltextDocument"
    output_xml_path.parent.mkdir(parents=True, exist_ok=True)

    last_err: Optional[Exception] = None

    for attempt in range(1, retries + 1):
        try:
            with open(pdf_path, "rb") as f:
                files = {"input": (pdf_path.name, f, "application/pdf")}
                data = {
                    # Basic, safe defaults; tune if needed
                    "consolidateHeader": 1,
                    "consolidateCitations": 0,
                }
                resp = requests.post(url, files=files, data=data, timeout=300)

            resp.raise_for_status()

            text = resp.text.strip()
            # Basic sanity check: Grobid should return a TEI XML document
            if not text or "<TEI" not in text:
                raise ValueError("Grobid returned empty or non-TEI response")

            with open(output_xml_path, "w", encoding="utf-8") as out_f:
                out_f.write(text)

            return

        except RequestException as e:
            last_err = e
            if attempt < retries:
                time.sleep(backoff * attempt)
            else:
                raise


def safe_title(raw_title: str) -> str:
    """Return a filesystem-safe version of the title for metadata only."""
    # Keep this simple; we don't use it for filenames, just JSON.
    return raw_title.strip()


def parse_pdfs_directory(
    input_dir: Path, output_root: Path = OUTPUT_ROOT
) -> None:
    """
    Parse all PDFs in a local directory with Grobid.

    For each `<name>.pdf` in `input_dir`, we create a folder
    `<output_root>/<name>/` and write `<name>.tei.xml` there, plus a simple
    `meta.json`.
    """
    output_root.mkdir(parents=True, exist_ok=True)

    pdf_paths = sorted(input_dir.glob("*.pdf"))
    if not pdf_paths:
        print(f"No PDFs found in {input_dir}")
        return

    print(f"Using Grobid at: {GROBID_URL}")
    print(f"Grobid sleep between calls: {GROBID_SLEEP} seconds")
    print(f"Parsing PDFs from folder: {input_dir}")
    print(f"Writing per-paper folders under: {output_root}")

    errors_parse: list[tuple[int, str, str]] = []

    for idx, pdf_path in enumerate(
        tqdm(pdf_paths, desc="Parsing local PDFs with Grobid"), start=1
    ):
        folder_name = pdf_path.stem
        paper_dir = output_root / folder_name
        tei_path = paper_dir / f"{folder_name}.tei.xml"
        meta_path = paper_dir / "meta.json"

        try:
            # Always run Grobid (overwrite if already exists)
            grobid_process_fulltext(pdf_path, tei_path)
            if GROBID_SLEEP > 0:
                time.sleep(GROBID_SLEEP)

            if not meta_path.exists():
                meta = {
                    "paper_id": folder_name,
                    "forum": "",
                    "title": safe_title(folder_name),
                    "pdf_url": "",
                    "source_path": str(pdf_path),
                }
                paper_dir.mkdir(parents=True, exist_ok=True)
                with open(meta_path, "w", encoding="utf-8") as f:
                    json.dump(meta, f, ensure_ascii=False, indent=2)
        except Exception as e:  # noqa: BLE001
            errors_parse.append((idx, folder_name, repr(e)))

    total = len(pdf_paths)
    print(f"\nDone parsing local PDFs. Total PDFs: {total}")
    print(f"Parse errors: {len(errors_parse)}")

    if errors_parse:
        error_log_path = output_root / "grobid_errors_local.json"
        with open(error_log_path, "w", encoding="utf-8") as f:
            json.dump(
                {
                    "parse_errors": errors_parse,
                },
                f,
                ensure_ascii=False,
                indent=2,
            )
        print(f"Error details written to: {error_log_path}")


def main() -> None:
    """
    Entry point: parse all PDFs in the local `pdfs/` folder.

    Expects a structure like:
        OB639-LLM-ABM-Simulation/
          pdfs/
            PAPER1.pdf
            PAPER2.pdf
          pdfs_parsed/
            ...
    """
    # Parse PDFs from the local `pdfs/` folder.
    local_pdfs_dir = REPO_ROOT / "pdfs"
    if not local_pdfs_dir.exists():
        raise FileNotFoundError(f"No local pdfs/ folder found at {local_pdfs_dir}")

    parse_pdfs_directory(local_pdfs_dir, OUTPUT_ROOT)


if __name__ == "__main__":
    main()

