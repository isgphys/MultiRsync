MultiRsync
==========

MultiRsync is a wrapper for rsync. It queues the subfolders of a given source path and lets multiple rsync processes work them off. It is developed and used by the [IT Services Group](http://isg.phys.ethz.ch) of the Physics Department at ETH Zurich.

Usage
-----

```sh
multirsync.pl [OPTIONS] <source> <destination>
```

The source and destination use the same syntax as `rsync`.

Optional Arguments:

    --pattern <string>         # use find option -name
    --delete                   # use rsync option --delete
    --inplace                  # use rsync option --inplace
    --relative                 # use rsync option --relative
    --size-only                # use rsync option --size-only
    --exclude-from <file>      # use rsync option --exclude-from
    --th <nr>                  # Number of threads, Default: 1
    -e | --rsh=<rsh|ssh>       # specify the remote shell to use, default = ssh
    -n | --dry-run             # dry-run without making changes (implies verbose)
    -v | --verbose             # verbose mode
    --version                  # see version
    --help                     # see this help

License
-------

> MultiRsync
>
> Copyright 2015 Patrick Schmid
>
> This program is free software: you can redistribute it and/or modify
> it under the terms of the GNU General Public License as published by
> the Free Software Foundation, either version 3 of the License, or
> (at your option) any later version.
>
> This program is distributed in the hope that it will be useful,
> but WITHOUT ANY WARRANTY; without even the implied warranty of
> MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
> GNU General Public License for more details.
