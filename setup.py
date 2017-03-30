from distutils.core import setup

from Cython.Build import cythonize

version="0.0.1"

setup(
    name="gfspy",
    packages=["gtfspy"],
    version=version,
    description="Python package for analyzing public transport timetables",
    author="Rainer Kujala",
    author_email="Rainer.Kujala@gmail.com",
    url="https://github.com/CxAalto/gtfspy",
    download_url="https://github.com/CxAalto/gtfspy/archive/" + version + ".tar.gz",
    ext_modules=cythonize("gtfspy/routing/*.pyx"),
    keywords = ['transit', 'routing' 'gtfs', 'public transport', 'analysis', 'visualization'], # arbitrary keywords
)
