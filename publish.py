# Standard libraries
import os
# Third-party libraries
from cfis_utils import PublishUtils

if __name__ == "__main__":
    cwd = os.getcwd()
    PublishUtils.publish_new_python_package_version(
        toml_file_path=os.path.join(cwd, "pyproject.toml"),
        readme_file_path=os.path.join(cwd, "README.md"),
        repository_path=cwd,
        requirements_path=os.path.join(cwd, "requirements.txt")
    )
    