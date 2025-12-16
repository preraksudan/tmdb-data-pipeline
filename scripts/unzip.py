# scripts/unzip.py

import zipfile
import os

# Get the directory where the current script (unzip.py) is located
script_dir = os.path.dirname(os.path.abspath(__file__))

# Define base paths relative to the script location, then navigate up and down
# Go up one level to the project root and then into raw_dataset
credits_zip_path = os.path.join(script_dir, "../raw_dataset/tmdb_5000_credits.csv.zip")
movies_zip_path = os.path.join(script_dir, "../raw_dataset/tmdb_5000_movies.csv.zip")

# Go up one level to the project root and then into the unzipped directory
extract_dir = os.path.join(script_dir, "../unzipped")

def unzip_file(zip_path, destination_path):
    """Uncompresses a .zip file."""
    # Ensure destination directory exists
    if not os.path.exists(destination_path):
        os.makedirs(destination_path)
        print(f"Created directory: {destination_path}")

    # Use zipfile module to extract
    try:
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            print(f"Extracting '{os.path.basename(zip_path)}' to '{destination_path}'...")
            zip_ref.extractall(destination_path)
            print("Extraction complete.")
    except FileNotFoundError:
        print(f"Error: Zip file not found at '{zip_path}'")
    except zipfile.BadZipFile:
        print(f"Error: '{zip_path}' is not a valid zip file.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

# Run the extraction for both files
if __name__ == "__main__":
    unzip_file(credits_zip_path, extract_dir)
    print("-" * 20)
    unzip_file(movies_zip_path, extract_dir)

