# Git-datashelf

Git-datashelf is a Python library for dealing with data versioning using git

## Installation

```bash
git clone https://github.com/geiges/git_datashelf.git
cd git_datashelf
pip install -e .
```

## Usage

```python
import git_datashelf

repm = GitRepository_Manager(
    path_to_repo =  'your/path/to/datafolder',
    debugmode=False
    )

```

## Contributing

Pull requests are welcome. For major changes, please open an issue first
to discuss what you would like to change.

Please make sure to update tests as appropriate.

## License

[MIT](https://choosealicense.com/licenses/mit/)
