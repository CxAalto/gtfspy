import glob
from subprocess import call

notebooks = glob.glob('*.ipynb')
for notebook in notebooks:
    call(["runipy", notebook])
