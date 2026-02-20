import csv
from pathlib import Path

INPUT_OVER15 = Path("input/Over15.csv")
INPUT_OVER05 = Path("input/Over05_1h.csv")


def extract_matches(csv_path: Path):
    matches = []

    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.reader(f)

        for row in reader:
            # necesitamos al menos 2 columnas
            if len(row) < 2:
                continue

            match = row[1].strip()

            # saltar basura
            if not match:
                continue
            if match.lower().startswith("filtro"):
                continue
            if " vs " not in match:
                continue

            matches.append(match)

    return matches


def main():
    over15 = extract_matches(INPUT_OVER15)
    over05 = extract_matches(INPUT_OVER05)

    print(f"Over 1.5 matches: {len(over15)}")
    print(f"Over 0.5 1H matches: {len(over05)}")

    print("\nSample Over 1.5:")
    for m in over15[:5]:
        print(" -", m)

    print("\nSample Over 0.5 1H:")
    for m in over05[:5]:
        print(" -", m)


if __name__ == "__main__":
    main()
