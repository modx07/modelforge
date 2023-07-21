from setuptools import setup, find_packages

setup(
    name='modelforge',
    version='0.1.0',
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
        'Click',
        'GitPython',
        'pandas',
        'requests',
        'dask',
        'dask[distributed]',
    ],
    entry_points={
        'console_scripts': [
            'modelforge = modelforge.cli',
        ],
    },
)