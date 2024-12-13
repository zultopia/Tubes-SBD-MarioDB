class PrimaryKey:
    def __init__(self, *keys):
        # keys nya langsung value si primary key nya aja
        self.isComposite = len(keys) > 1
        self.keys = keys

    def __eq__(self, other):
        if not isinstance(other, PrimaryKey):
            return NotImplemented
        if(self.isComposite != other.isComposite):
            return False
        return self.keys == other.keys

    def __ne__(self, other):
        return not self.__eq__(other)
    
    def __hash__(self):
        concatted_keys = ""
        for key in self.keys:
            concatted_keys += str(key)
        return hash(concatted_keys)

    def __str__(self):
        return (
            f"===== PrimaryKey =====\nisComposite: {self.isComposite}\nkeys: {self.keys}\n======================\n"
        )

class IDataItem:
    def get_parent(self):
        pass
    
class Database(IDataItem):
    def get_parent(self):
        return None

class Table(IDataItem):
    def __init__(self, table: str):
        self.table = table
    
    def __eq__(self, other):
        if isinstance(other, Table):
            return self.table == other.table
        return False

    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        return hash(self.table)
    
    def __str__(self):
        return self.table
    
class Row(IDataItem):
    def __init__(self, table: Table, pkey: PrimaryKey, map: dict):
        self.table = table
        self.pkey = pkey
        self.map = map
        
    def __getitem__(self, key):
        # use case: Row['key1'], then it will return the value of that 'key1' key in the map
        if key in self.map:
            return self.map[key]
        raise KeyError(f"Key '{key}' not found")
    
    def __eq__(self, other):
        if isinstance(other, Row):
            return (self.pkey == other.pkey) and (self.table == other.table)
        return False
    
    def __ne__(self, other):
        return not self.__eq__(other)
    
    def __str__(self):
        return f"=============== Row ===============\ntable: {self.table}\nmap:\n{self.map}\npkey:\n{self.pkey}===================================\n"

    def __hash__(self):
        return hash(self.table.__str__() + self.pkey.__str__())
    
    def get_table(self): 
        return self.table
    
    def get_parent(self):
        return self.get_table()

class Cell(IDataItem):
    def __init__(self, table: Table, row: Row, pkey: PrimaryKey, attribute: str, value: any):
        self.row = row
        self.table = table
        self.pkey = pkey
        self.attribute = attribute
        self.value = value
    
    def __eq__(self, other):
        if isinstance(other, Cell):
            return (self.pkey == other.pkey) and (self.attribute == other.attribute) and (self.table == other.table)
        return False

    def __hash__(self):
        return hash(self.table.table + self.pkey.__str__() + self.attribute)
    
    def get_row(self):
        return self.row
    
    def get_table(self):
        return self.row.get_table()

    def get_parent(self):
        return self.get_row()

class DataItem:
    def __init__(self, level: str, data_item):
        self.level = level
        self.data_item = data_item
        
    def __eq__(self, other):
        if isinstance(other, DataItem):
            return (self.level == other.level) and (self.data_item == other.data_item)
        return False
    
    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        return self.data_item.__hash__()
    
    def get_parent(self):
        return self.data_item.get_parent()

class Action:
    def __init__(self, action : str):
        self.action = action 
        
    def __str__(self):
        return f"=== Action ===\naction: {self.action}\n==============\n"
        
class Response:
    def __init__(self, allowed: bool, status: str, transaction_id: int, message: str = ""):
        self.allowed = allowed
        self.status = status
        self.transaction_id = transaction_id
        self.message = message
        
    def __str__(self):
        return f"==== Response ====\nallowed: {self.allowed}\ntransaction_id: {self.transaction_id}\n==================\n"

class Lock:
    def __init__(self, type: str, transaction_id: int, row: Row):
        # type is either 'S' or 'X'
        self.type = type.upper()
        if(self.type != 'S' and self.type != 'X'):
            raise ValueError(f"Invalid lock type: {type}. Allowed values are 'S' or 'X'.")
        self.transaction_id = transaction_id
        self.row = row
        
    def __str__(self):
        return f"=============== Lock ===============\ntype: {self.type}\ntransaction_id: {self.transaction_id}\nrow:\n{self.row}====================================\n"    


class TransactionAction: 
    def __init__(self, tid: int, action: Action, level: str, data_item: DataItem, old_data_item: DataItem = None): 
        self.id = tid 
        self.action = action
        self.level  = level
        self.data_item = data_item
        self.old_data_item = old_data_item

class WaitForGraph:
    def __init__(self):
        self.waitfor = {}   # tid_waiting: set[tid_waited]
    
    def addEdge(self, tid_waiting: int, tid_waited: int):
        if tid_waiting not in self.waitfor: 
            self.waitfor[tid_waiting] = set()
        self.waitfor[tid_waiting].add(tid_waited)
    
    def deleteEdge(self, tid_waiting: int, tid_waited: int):
        if tid_waiting in self.waitfor and tid_waited in self.waitfor[tid_waiting]:
            self.waitfor[tid_waiting].remove(tid_waited)
            if not self.waitfor[tid_waiting]: 
                del self.waitfor[tid_waiting]
        
    def deleteNode(self, tid): 
        # remove edges that pointing to tid node 
        for tid_waiting in list(self.waitfor):
            if tid in self.waitfor[tid_waiting]: 
                self.waitfor[tid_waiting].remove(tid)

                if not self.waitfor[tid_waiting]: 
                    del self.waitfor[tid_waiting]

        # remove the tid node
        if tid in self.waitfor: 
            del self.waitfor[tid]
                
    def isCyclic(self):
        def dfs(node, visited, rec_stack): 
            visited.add(node)
            rec_stack.add(node)

            for neighbor in self.waitfor.get(node, []): 
                if neighbor not in visited: 
                    if dfs(neighbor, visited, rec_stack): 
                        return True
                elif neighbor in rec_stack: 
                    return True
                
            rec_stack.remove(node)
            return False

        visited = set()
        rec_stack = set()

        for node in self.waitfor: 
            if node not in visited: 
                if dfs(node, visited, rec_stack): 
                    return True
        
        return False
    
    def waiting(self, tid: int): 
        return tid in self.waitfor

    def __str__(self):
        return f"{self.waitfor}"