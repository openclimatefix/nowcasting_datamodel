""" Usual setup file for package """
from pathlib import Path

from setuptools import find_packages, setup

this_directory = Path(__file__).parent
install_requires = (this_directory / "requirements.txt").read_text().splitlines()
long_description = (this_directory / "README.md").read_text()
#

with open("nowcasting_datamodel/__init__.py") as f:
    for line in f:
        if line.startswith("__version__"):
            _, _, version = line.replace("'", "").split()
            version = version.replace('"', "")


setup(
    name="nowcasting_datamodel",
    packages=find_packages(),
    version=version,
    license="MIT",
    description="Data Model for the OCF nowcasting project",
    author="Peter Dudfield",
    author_email="peter@openclimatefix.org",
    company="Open Climate Fix Ltd",
    url="https://github.com/openclimatefix/nowcasting_datamodel",
    keywords=[
        "SQL",
        "Datamodel",
    ],
    install_requires=install_requires,
    long_description=long_description,
    long_description_content_type="text/markdown",
)
