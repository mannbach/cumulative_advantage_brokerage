"""Script to install local packages
"""
import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name="cumulative_advantage_brokerage",
    author="Jan Bachmann",
    author_email="bachmann@csh.ac.at",
    description=("Code to reproduce findings of the paper ``Cumulative Advantage of Brokerage''."),
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/mannbach/cumulative_advantage_brokerage",
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
    package_dir={"": "./"},
    packages=setuptools.find_packages(),
    python_requires=">=3.9",
)
