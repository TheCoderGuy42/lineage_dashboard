from setuptools import setup, find_packages

setup(
    name="lineage",                   # Package name
    version="0.1.0",
    author="Abilash",
    author_email="abilash.suresh199@gmail.com",
    description="Something something datalake",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    python_requires=">=3.7",
    install_requires=[
        # add any runtime dependencies here
    ],
    entry_points={
        # Optional: uncomment and adjust if you want a console script
        'console_scripts': [
            'lineage = lineage.cli:main',
        ],
    },
)
