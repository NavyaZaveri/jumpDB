import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="jumpDB",  # Replace with your own username
    version="0.0.1",
    author="Navya Zaveri",
    author_email="author@example.com",
    description="A simple kv store ",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/NavyaZaveri/jumpDB",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: BSD 2-Clause",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
)
