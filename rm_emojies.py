# import pathlib
# import unicodedata
# import argparse
# import sys

# def filter_to_clean_text(text):
#     """
#     Strictly keeps only letters, numbers, punctuation, and whitespace.
#     This nukes emojis (Symbols), combining marks (Decorations), and 
#     control characters (Gibberish).
#     """
#     # Normalize to decomposed form to separate base characters from 
#     # combining marks (like the 'keycap' on your 8 emoji).
#     normalized_text = unicodedata.normalize('NFD', text)
    
#     # We define the allowed categories:
#     # L = Letters (Any language)
#     # N = Numbers
#     # P = Punctuation
#     # Z = Separators (Spaces)
#     # We also explicitly allow newlines and tabs.
#     allowed_categories = ('L', 'N', 'P', 'Z')
    
#     clean_chars = []
#     for char in normalized_text:
#         category = unicodedata.category(char)
        
#         # Check if the character belongs to an allowed category
#         # or is a necessary control character like a newline.
#         if category[0] in allowed_categories or char in ('\n', '\r', '\t'):
#             clean_chars.append(char)
    
#     # Re-normalize back to composed form (NFC) for the final output
#     result = "".join(clean_chars)
#     return unicodedata.normalize('NFC', result)

# def process_files(input_dir, output_dir):
#     """
#     Walks the directory tree and processes .md and .txt files.
#     """
#     input_path = pathlib.Path(input_dir).resolve()
#     output_path = pathlib.Path(output_dir).resolve()

#     if not input_path.is_dir():
#         print(f"Error: The input path '{input_path}' is not a directory.")
#         sys.exit(1)

#     print(f"Scanning: {input_path}")
#     print(f"Outputting to: {output_path}")

#     files_processed = 0
#     for item in input_path.rglob('*'):
#         if item.is_file() and item.suffix.lower() in ('.md', '.txt'):
#             # Calculate the relative path to maintain folder structure
#             relative_item_path = item.relative_to(input_path)
#             target_file_path = output_path / relative_item_path

#             # Create subdirectories in the output folder as needed
#             target_file_path.parent.mkdir(parents=True, exist_ok=True)

#             try:
#                 # Read original file
#                 content = item.read_text(encoding='utf-8', errors='replace')
                
#                 # Strip the junk
#                 cleaned_content = filter_to_clean_text(content)
                
#                 # Save the cleaned version
#                 target_file_path.write_text(cleaned_content, encoding='utf-8')
#                 print(f"Cleaned: {relative_item_path}")
#                 files_processed += 1
                
#             except Exception as e:
#                 print(f"Failed to process {item.name}: {e}")

#     print(f"\nFinished. Processed {files_processed} files.")
#     print("All fancy unicode decorations and emojis have been discarded.")

# def main():
#     parser = argparse.ArgumentParser(
#         description="Recursively remove all non-text/punctuation/number characters from files."
#     )
#     parser.add_argument("input_dir", help="Path to the directory containing dirty files.")
#     parser.add_argument("output_dir", help="Path where the cleaned files will be saved.")
    
#     args = parser.parse_args()

#     process_files(args.input_dir, args.output_dir)

# if __name__ == "__main__":
#     main()

import pathlib
import unicodedata
import argparse
import sys

def deep_clean_text(text):
    """
    Decomposes Unicode characters and keeps only letters, numbers, 
    punctuation, and whitespace. Everything else is discarded.
    """
    # NFD (Normalization Form Decomposition) breaks "8️⃣" into 
    # the character '8' and the combining symbols.
    decomposed_text = unicodedata.normalize('NFD', text)
    
    # Allowed categories:
    # L = Letters (All languages)
    # N = Numbers
    # P = Punctuation
    # Z = Separators (Spaces)
    allowed_categories = ('L', 'N', 'P', 'Z')
    
    # Explicitly keep essential whitespace characters
    allowed_control = ('\n', '\r', '\t')
    
    clean_chars = []
    for char in decomposed_text:
        category = unicodedata.category(char)
        
        # We keep the character only if it is a basic text category
        # or an allowed control character.
        if category[0] in allowed_categories or char in allowed_control:
            clean_chars.append(char)
    
    # Re-normalize back to NFC (Normal Form Composed) for the final string
    result = "".join(clean_chars)
    return unicodedata.normalize('NFC', result)

def run_cleaner(input_directory, output_directory):
    """
    Recursively scans the input directory and mirrors it in the output directory.
    """
    base_in = pathlib.Path(input_directory).resolve()
    base_out = pathlib.Path(output_directory).resolve()

    if not base_in.is_dir():
        print(f"Error: The source '{base_in}' is not a valid directory.")
        sys.exit(1)

    print(f"Purging symbols from: {base_in}")
    print(f"Saving clean files to: {base_out}")

    file_count = 0
    for file_path in base_in.rglob('*'):
        # Filter for the file types you specified
        if file_path.is_file() and file_path.suffix.lower() in ('.md', '.txt'):
            # Calculate the relative path to maintain folder hierarchy
            relative_path = file_path.relative_to(base_in)
            destination = base_out / relative_path

            # Ensure the output subdirectory exists
            destination.parent.mkdir(parents=True, exist_ok=True)

            try:
                # Read with UTF-8, replacing errors to prevent script crashes
                raw_content = file_path.read_text(encoding='utf-8', errors='replace')
                
                # Apply the whitelist filter
                sanitized_content = deep_clean_text(raw_content)
                
                # Write the output
                destination.write_text(sanitized_content, encoding='utf-8')
                print(f"Cleaned: {relative_path}")
                file_count += 1
            except Exception as error:
                print(f"Failed to process {file_path.name}: {error}")

    print(f"\nTask complete. {file_count} files processed.")
    print("All non-essential Unicode symbols have been eradicated.")

def main():
    parser = argparse.ArgumentParser(
        description="A strict script to remove all non-alphanumeric/punctuation symbols from files."
    )
    parser.add_argument("src", help="Input directory path")
    parser.add_argument("dest", help="Output directory path")
    
    args = parser.parse_args()

    # Pass the arguments to the runner
    run_cleaner(args.src, args.dest)

if __name__ == "__main__":
    main()
