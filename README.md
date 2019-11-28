Fork: Readaptation of the lib using tinydb as embedded db.
```bash
pip install -r requirements.txt
```

## Usage

```bash
A tool to find and remove duplicate pictures.

Usage:
    duplicate_finder.py add <path> ...
    duplicate_finder.py remove <path> ...
    duplicate_finder.py clear
    duplicate_finder.py show
    duplicate_finder.py find [--delete] [--filename]
    duplicate_finder.py -h | --help

Options:
    -h, --help                Show this screen

    find:
        --delete              Move all found duplicate pictures to the trash. This option takes priority over --print.
        --filename            Get all filename of duplicates
```
