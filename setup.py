import setuptools

setuptools.setup(
    name = "gwak",
    version = "0.6.1",
    author = "LiviaMedeiros",
    author_email = "livia@cirno.name",
    description = "Directory gwaking utility",
    long_description = "Gwak a directory by burying filebodies and replacing them with symlinks.",
    long_description_content_type = "text/plain",
    license = "GPLv3",
    url = "https://github.com/LiviaMedeiros/gwak",
    project_urls = {
        "Bug Tracker": "https://github.com/LiviaMedeiros/gwak/issues",
    },
    classifiers = [
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.10",
        "License :: OSI Approved :: GNU General Public License v3 (GPLv3)",
        "Operating System :: OS Independent",
    ],
    py_modules = [
        "gwak",
        "libgwak.manifest",
        "libgwak.zy"
    ],
    package_dir = {
        '': 'src'
    },
    entry_points = {
        'console_scripts': [
            'gwak = gwak:main'
        ]
    },
    python_requires = ">=3.10",
)