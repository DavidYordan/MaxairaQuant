python -m nuitka --lto=yes --follow-imports --include-data-files=config/app.yaml=config/app.yaml --output-dir=dist main.py


Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
venv\Scripts\activate.ps1