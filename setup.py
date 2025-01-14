from setuptools import setup

with open("README.md", "r") as fh:
    long_description = fh.read()

with open("requirements.txt") as f:
    required = f.read().splitlines()

setup(
    name="abn-amro-assessment-2024",
    version="0.0.5",
    packages=[
        "framework",
        "framework.base",
        "framework.reader",
        "framework.transform",
        "framework.writer",
    ],
    url="",
    license="",
    install_requires=required,
    author="Sumanta Dutta",
    author_email="sumanta.dutta2012@gmail.com",
    long_description=long_description,
    long_description_content_type="text/markdown",
    description="ABN Amro technical assignment package",
)
