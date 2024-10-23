from setuptools import setup

with open("requirements.txt") as f:
    required = f.read().splitlines()

setup(
    name="abn_amro_test",
    version="0.0.1",
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
    long_description="ABN Amro technical assignment package",
    long_description_content_type="text/markdown",
    description="ABN Amro technical assignment package",
)
