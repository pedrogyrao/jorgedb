import os
import json
from sys import getsizeof
from collections import namedtuple

# from jorgedb import DEFAULT_PATH


def append_spaces(string, size):
    return ('{:>'+ str(size) +'}').format(string)


class JorgeDB:
    def __init__(self, block_key_size=None, path=None):
        self._path = os.path.abspath(path)
        self._header = self._path + '\\header'
        self._blocks = self._path + '\\blocks'

        if block_key_size is None:
            with open(self._header, 'r') as f:
                block_key_size = int(f.readline()), int(f.readline())
                self._offset = f.tell()
        else:
            with open(self._header, 'w') as f:
                f.write('{}\n'.format(block_key_size[0]))
                f.write('{}\n'.format(block_key_size[1]))
                self._offset = f.tell()
        self._bsize, self._ksize = block_key_size

        self._init_map()

    def _init_map(self):
        self._map = {}
        with open(self._header, 'r') as header:
            header.seek(self._offset)
            block_pointer = 0
            while True:
                block_size = header.read(self._bsize)
                if len(block_size) == 0:
                    break
                block_size = int(block_size)
                key = header.read(self._ksize)
                self._map[key] = (block_pointer, block_size)
                block_pointer += block_size

    def _get_block(self, key):
        pointer, size = self._map[key]
        with open(self._blocks, 'r') as f:
            f.seek(pointer)
            block_string = f.read(size)
        return json.loads(block_string)

    def _format_data_to_save(self, key, block):
        block = json.dumps(block)
        block_size = append_spaces(len(block), self._bsize)
        key = append_spaces(key, self._ksize)
        return block_size + key, block

    def __contains__(self, key):
        return key in self._map

    def _set(self, key, block):
        to_header, to_blocks = self._format_data_to_save(key, block)
        with open(self._header, 'a') as header, open(self._blocks) as blocks:
            position = blocks.tell()
            header.write(to_header)
            blocks.write(to_blocks)
        self._map[key] = position

    def __setitem__(self, key, block):
        if key in self._map:
            raise KeyError(
                'Jorge located at "{}" already knows "{}" key.'.format(
                    self._path, key))
        self._set(key, block)

    def __getitem__(self, key):
        if key in self._map:
            return self._get_block(key)
        raise KeyError(
            'Jorge located at "{}" does not know key "{}".'.format(self._path, key))

    def _estimate_mem_consumption(self):
        map_len = len(self._map)
        map_size = getsizeof(self._map)
        key_size = self._ksize + getsizeof(str())
        tuple_size = getsizeof(tuple()) + 2 * (getsizeof(int()) + 8)
        return map_len * (key_size + tuple_size) + map_size

    def _estimate_disck_consumption(self):
        db_filesize = os.stat(self._header).st_size
        db_filesize += os.stat(self._blocks).st_size
        return db_filesize

    def __str__(self):
        db_block_count = len(self._map)
        db_memory_allocated = self._estimate_mem_consumption() / float(1 << 20)
        db_filesize = self._estimate_disck_consumption() / float(1 << 20)

        message = ['Jorge located at "{}":'.format(self._path)]
        message.append('Block count: {}'.format(db_block_count))
        message.append('Memory use: {:,.1f} MB'.format(db_memory_allocated))
        message.append('Disck consumption: {:,.1f} MB'.format(db_filesize))
        return '\n\t'.join(message)
