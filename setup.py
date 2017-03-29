from distutils.core import setup

from Cython.Build import cythonize

setup(
    name="gfspy",
    packages=["gtfspy"],
    version="0.1",
    description="Python package for analyzing public transport timetables",
    author="Rainer Kujala",
    author_email="Rainer.Kujala@gmail.com",
    url="https://github.com/CxAalto/gtfspy",
    download_url="https://github.com/CxAalto/gtfspy/archive/0.2.tar.gz",
    ext_modules=cythonize("gtfspy/routing/*.pyx"),
    keywords = ['transit', 'routing' 'gtfs', 'public transport', 'analysis', 'visualization'], # arbitrary keywords
)
