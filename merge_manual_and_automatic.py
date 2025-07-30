import os
import json

MANUAL_DIR = "manual"
AUTOMATIC_DIR = "automatic"
FINAL_DIR = "final"

os.makedirs(FINAL_DIR, exist_ok=True)

def load_json(path):
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}

def merge_language_file(filename):
    manual_path = os.path.join(MANUAL_DIR, filename)
    automatic_path = os.path.join(AUTOMATIC_DIR, filename)
    final_path = os.path.join(FINAL_DIR, filename)

    manual = load_json(manual_path)
    automatic = load_json(automatic_path)

    # Manual lists
    manual_dubbed = set(manual.get("dubbed", []))
    manual_not_dubbed = set(manual.get("not_dubbed", []))
    manual_incomplete = set(manual.get("incomplete", []))

    # Automatic list
    auto_dubbed = set(automatic.get("dubbed", []))

    # Merge logic
    final_dubbed = (auto_dubbed | manual_dubbed) - manual_not_dubbed - manual_incomplete
    final_incomplete = manual_incomplete

    result = {
        "_license": "This file is licensed under the MIT License. User visible attribution is required.",
        "_origin": "https://github.com/Joelis57/MyDubList",
        "language": manual["language"],
        "dubbed": sorted(final_dubbed),
        "incomplete": sorted(final_incomplete)
    }

    with open(final_path, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    print(f"Merged: {filename} → {FINAL_DIR}")

def main():
    if not os.path.exists(MANUAL_DIR):
        print(f"Manual directory '{MANUAL_DIR}' does not exist.")
        return

    for filename in os.listdir(MANUAL_DIR):
        if filename.endswith(".json"):
            merge_language_file(filename)

if __name__ == "__main__":
    main()
