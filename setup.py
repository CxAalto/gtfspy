from setuptools import setup, Extension, find_packages

version="0.0.4"

setup(
    name="gtfspy",
    version=version,
    description="Python package for analyzing public transport timetables",
    url="https://github.com/CxAalto/gtfspy",
    packages=find_packages(exclude=["java_routing", "examples"]),
    author="Rainer Kujala",
    author_email="Rainer.Kujala@gmail.com",
    license='MIT',
    classifiers=[
        # How mature is this project? Common values are
        #   3 - Alpha
        #   4 - Beta
        #   5 - Production/Stable
        'Development Status :: 3 - Alpha',

        # Indicate who your project is intended for
        'Intended Audience :: Developers',
        'Intended Audience :: Science/Research',
        'Topic :: Scientific/Engineering :: GIS',

        # Pick your license as you wish (should match "license" above)
         'License :: OSI Approved :: MIT License',

        # Specify the Python versions you support here. In particular, ensure
        # that you indicate whether you support Python 2, Python 3 or both.
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.5'
    ], 
    install_requires = [
        "setuptools>=18.0",
        "pandas",
        "networkx==1.11",
        "pyshp",
        "smopy",
        "nose",
        "Cython",
        "six",
        "geoindex",
        "osmread==0.2",
        "shapely",
        "geojson>=2.0.0",
        "pyproj",
        "matplotlib-scalebar==0.6.1"
    ],
    ext_modules=[
        Extension(
            'gtfspy.routing.label',
            sources=["gtfspy/routing/label.pyx"],
        ),
    ],
    keywords = ['transit', 'routing' 'gtfs', 'public transport', 'analysis', 'visualization'], # arbitrary keywords
)
