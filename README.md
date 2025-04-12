# cfis-interfaces
Interfaces used by our lab to control peripherals such as detectors or 3D printers.

# How to install

You can install the library using pip:
```bash
pip install git+https://github.com/CFIS-UFRO/cfis-interfaces.git
```

Also you can specify a specific version to install:
```bash
pip install git+https://github.com/CFIS-UFRO/cfis-interfaces.git@<version>
```
where `<version>` is one of the [available tags](https://github.com/CFIS-UFRO/cfis-interfaces/tags).

**Latest stable tag**: v2025.04.12.02

# For developers

- The idea is to keep this library compatible with Python 3.8 and above.
- For development it is recommended to use a virtual environment with conda and install all the dependencies in it.
    ```bash
    conda create -n cfis-interfaces python=3.8
    conda activate cfis-interfaces
    pip install -r requirements.txt
    ```
- Any time you use a new dependency, please add it to these files too:
    - `requirements.txt`
    - `pyproject.toml`
- The library versions are based on tags, to publish a new version run the script `publish.py`:
    ```bash
    python publish.py
    ```
- To run individual files for testing, you can use the `-m` flag:
    ```bash
    python -m src.cfis_interfaces.<file without .py>
    ```