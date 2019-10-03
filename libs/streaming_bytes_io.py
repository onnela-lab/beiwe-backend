from io import BytesIO


class StreamingBytesIO(BytesIO):
    """
    Implementation of BytesIO that allows us to keep track of the stream's virtual position
    while simultaneously emptying the stream as we go, allowing for streaming zip file creation.

    Python 3 update: some of the internals of Zipfile changed, added seek function
    """
    _position = 0

    def empty(self):
        """ Clears the BytesIO object while retaining the current virtual position """
        self._position = self.tell()
        # order does not matter for truncate and seek
        self.truncate(0)
        super(StreamingBytesIO, self).seek(0)

    def tell(self):
        """ Returns the current stream's virtual position (where the stream would be if it had
        been running contiguously and self.empty() is not called) """
        return self._position + super(StreamingBytesIO, self).tell()

    def seek(self, *args, **kwargs):
        """ Sets the position explicitly, required for compatibility with Python 3 Zipfile """
        self._position = args[0]
        return super(StreamingBytesIO, self).seek(0)
