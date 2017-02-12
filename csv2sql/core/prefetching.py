"""File pre-fetching."""

import itertools
import tempfile
from builtins import object


class RewindableFileIterator(object):
    """A file iterator class that can be rewinded.
    An instances of this class can create a temporary file
    and should be closed by `close()` or using `with` statement.
    """

    def __init__(self, file_obj, **kwargs):
        """Initialize.
        The buffer size can be specified by `buffer_size`,
        which can result in performance improvement
        in exchange for memory usage.
        """
        buffer_size = kwargs.get('buffer_size', 10 * 1024 * 1024)

        self._file = file_obj
        self._buffer = tempfile.SpooledTemporaryFile(
            max_size=buffer_size, mode='w+')

    def __iter__(self):
        return self

    def __next__(self):
        try:
            return next(iter(self._buffer))
        except StopIteration:
            line = next(self._file)
            self._buffer.write(line)
            return line

    def __enter__(self):
        return self

    def __exit__(self, *args, **kwarg):
        self.close()

    @property
    def closed(self):
        """Return if the temporary file is closed or not."""
        return self._buffer.closed

    def close(self):
        """Close the temporary file."""
        self._buffer.close()

    def rewind(self):
        """Rewind the file to its head."""
        self._buffer.flush()
        self._buffer.seek(0)

    def freeze(self):
        """Stop iteration and return the iterator of the rest
        of the file, which stops the file writing and results
        in performance improvement in exchange of disabling file
        rewinding.
        """
        buf = self._buffer
        return itertools.chain(iter(buf), self._file)
