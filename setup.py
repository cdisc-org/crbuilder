from setuptools import setup, find_packages

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
    name="rulebuilder",
    version="0.3.0",
    author="Hanming Tu",
    author_email="hanming.tu@gmail.com",
    description="A tool for building CDISC Core rules",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/htu/core-rule-builder/rulebuilder",
    packages=find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.6",
    install_requires=[
        "pandas==1.5.3",
        "ruamel.yaml == 0.17.21"
    ],
)
