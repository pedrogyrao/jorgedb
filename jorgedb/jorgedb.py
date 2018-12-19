import os
import json
from collections import namedtuple


def append_spaces(string, size):
    return ('{:>'+ str(size) +'}').format(string)


class JorgeDB:
    def __init__(self, block_key_size=None, path='jdb'):
        self._path = path

        if block_key_size is None:
            with open(self._path, 'r') as f:
                block_key_size = int(f.readline()), int(f.readline())
                self._offset = f.tell()
        else:
            with open(self._path, 'w') as f:
                f.write('{}\n'.format(block_key_size[0]))
                f.write('{}\n'.format(block_key_size[1]))
                self._offset = f.tell()
        self._bsize, self._ksize = block_key_size

        self._init_map()

    def _init_map(self):
        self._map = {}
        with open(self._path, 'r') as f:
            f.seek(self._offset)
            while True:
                position = f.tell()
                block_size = f.read(self._bsize)
                if len(block_size) == 0:
                    break
                block_size = int(block_size)
                key = f.read(self._ksize)
                self._map[key] = position
                position = f.tell()
                f.seek(position + block_size)

    def _get_block(self, pointer):
        with open(self._path, 'r') as f:
            f.seek(pointer)
            block_size = int(f.read(self._bsize))
            f.seek(f.tell() + self._ksize)
            block_string = f.read(block_size)
        return json.loads(block_string)

    def _format_data_to_save(self, key, block):
        block = json.dumps(block)
        block_size = append_spaces(len(block), self._bsize)
        key = append_spaces(key, self._ksize)
        return block_size + key + block, len(block)

    def __contains__(self, key):
        return key in self._map

    def set(self, key, block):
        data, _ = self._format_data_to_save(key, block)
        with open(self._path, 'a') as f:
            position = f.tell()
            f.write(data)
        self._map[key] = position

    def __setitem__(self, key, block):
        if key in self._map:
            raise KeyError(
                'Jorge located at "{}" already knows "{}" key.'.format(
                    self._path, key))
        self.set(key, block)

    def __getitem__(self, key):
        try:
            position = self._map[key]
        except KeyError:
            raise KeyError(
                'Jorge located at "{}" does not know key "{}".'.format(
                    self._path, key))
        return self._get_block(position)
