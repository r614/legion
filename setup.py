from setuptools import setup, find_packages

setup(
    name="legion",
    version="0.0.1",
    description="Python Queueing Framework backed by PostgreSQL",
    include_package_data=True,
    author="Roshan Pawar",
    author_email="roshan@r614.dev",
    license="BSD 2-clause",
    packages=find_packages(),
    install_requires=["psycopg[binary]"],
    classifiers=[
        "Programming Language :: Python :: 3.11",
    ],
)
