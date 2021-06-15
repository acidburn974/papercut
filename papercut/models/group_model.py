# Copyright (c) 2002 Joao Prado Maia. See the LICENSE file for more information.

import papercut.storage.mysql as Papercut_Storage

        
class Fish:
    def __init__(self, first_name, last_name="Fish"):
        self.first_name = first_name
        self.last_name = last_name
        