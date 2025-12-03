# CFIS Interfaces

> **⚠️ NOTICE: This repository is no longer maintained**
>
> This project has been discontinued in favor of [CFIS-Commons](https://github.com/CFIS-UFRO/CFIS-Commons).

Interfaces used by the CFIS laboratory to control peripherals such as detectors or 3D printers.

# Requirements

*   Python 3.8 or higher.
    *   Windows: [https://www.python.org/](https://www.python.org/)
    *   Linux and MacOS: Usually pre-installed.
*   Git.
    *   Windows: [https://git-scm.com/download/win](https://git-scm.com/download/win)
    *   Linux: Install it through your package manager, e.g., `sudo apt install git`.
    *   MacOS: Install it through the Xcode command line tools using `xcode-select --install`.

# How to install

You can install the library using pip:
```bash
pip install git+https://github.com/CFIS-UFRO/cfis-interfaces.git
```

Additionally, you can specify a particular version to install:
```bash
pip install git+https://github.com/CFIS-UFRO/cfis-interfaces.git@<version>
```
where `<version>` is one of the [available tags](https://github.com/CFIS-UFRO/cfis-interfaces/tags).

**Latest stable tag**: v2025.09.23.01

# Interfaces

Each interface has its own README file with instructions on how to use it inside the `src/cfis_interfaces/` folder.

After installing the library, you can import the interfaces like this:
```python
from cfis_interfaces import <InterfaceName>
```

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
- Not only the tags but also the main branch should be stable. If you are planning to make big and possibly breaking changes, please create a new branch and merge it to the main branch when you are done.