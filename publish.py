# Standard libraries
import os
# Third-party libraries
from cfis_utils.PublishUtils import PublishUtils

if __name__ == "__main__":
    PublishUtils.publish_new_python_package_version(
        toml_file_path=os.path.join(os.getcwd(), "pyproject.toml"),
        readme_file_path=os.path.join(os.getcwd(), "README.md"),
        repository_path=os.getcwd()
    )
    