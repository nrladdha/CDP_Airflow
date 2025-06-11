from setuptools import setup, find_packages

setup(
    name="cdp-airflow",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "apache-airflow>=2.5.0",
        "cdpcli>=0.1.0",
    ],
    description="Airflow operator for managing CDP clusters",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/nrladdha/CDP_Airflow",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: Applicable Open Source License: Apache License Version 2.0",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.7",
) 