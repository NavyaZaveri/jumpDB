import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

with open('requirements.txt') as f:
    required = f.read().splitlines()

setuptools.setup(
    name="jumpDB",  # Replace with your own username
    version="0.0.3",
    author="Navya Zaveri",
    author_email="author@example.com",
    description="A simple kv store ",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/NavyaZaveri/jumpDB",
    packages=setuptools.find_packages(),
    install_requires=required,
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
)
