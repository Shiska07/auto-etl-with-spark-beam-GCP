import setuptools

setuptools.setup(
    name="beam_etl",
    version="0.1.0",
    packages=setuptools.find_packages(include=["beam_etl", "beam_etl.*"]),
)
