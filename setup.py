from setuptools import setup, find_packages

setup(
    name="tv_lib",
    version="0.1",
    packages=find_packages(),
    install_requires=[
        "pandas",
        "websocket-client",
        "requests",
    ],
)
