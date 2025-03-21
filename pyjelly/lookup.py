import re
from collections import OrderedDict

class LookupTable(OrderedDict):
    def __init__(self, max_size, verbose = False):
        self.max_size = max_size
        self.counter = 1
        self.verbose = verbose
        super().__init__()

    def __getitem__(self, key):

        if key in self:
            self.move_to_end(key) 
            return super().__getitem__(key)
        return None

    def add_entry(self, key):
        if len(self) >= self.max_size:
            removed_item = self.popitem(last=False)
            value = removed_item[1]
        else:
            value = self.counter
            self.counter += 1
        
        if self.verbose:
            print(f"Put {key} as {value}")
        super().__setitem__(key, value)
        return value
