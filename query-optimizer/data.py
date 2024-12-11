import math
from typing import Literal, Union

# The number of bytes in a block
BLOCK_SIZE = 100

MOCK_DATA = {
    "student": {
        "n": 1000,
        "attributes": {
            "id": {"V": 1000, "index": "hash", "size": 4, "min": 1, "max": 1000},  # Primary Key
            "name": {"V": 800, "index": None, "size": 20},  # String, no min/max
            "dept_name": {"V": 10, "index": None, "size": 16},  # String, no min/max
            "tot_cred": {"V": 50, "index": None, "size": 4, "min": 0, "max": 200},
        }
    },
    "advisor": {
        "n": 500,
        "attributes": {
            "s_id": {"V": 1000, "index": "hash", "size": 4, "min": 1, "max": 1000},  # Primary Key
            "i_id": {"V": 500, "index": None, "size": 4, "min": 1, "max": 500},
        }
    },
    "course": {
        "n": 500,
        "attributes": {
            "course_id": {"V": 500, "index": "hash", "size": 8},  # Primary Key
            "title": {"V": 400, "index": None, "size": 32},
            "dept_name": {"V": 10, "index": None, "size": 16},
            "credits": {"V": 5, "index": None, "size": 2, "min": 1, "max": 8},
        }
    },
    "takes": {
        "n": 2000,
        "attributes": {
            "id": {"V": 1000, "index": "hash", "size": 4, "min": 1, "max": 1000},  # Primary Key
            "course_id": {"V": 500, "index": "hash", "size": 8},  # Primary Key
            "sec_id": {"V": 50, "index": "hash", "size": 2, "min": 1, "max": 50},  # Primary Key
            "semester": {"V": 4, "index": "hash", "size": 1, "min": 1, "max": 4},
            "year": {"V": 10, "index": "hash", "size": 2, "min": 2020, "max": 2030},
            "grade": {"V": 5, "index": None, "size": 1, "min": 0, "max": 4},
        }
    },
    "instructor": {
        "n": 300,
        "attributes": {
            "id": {"V": 300, "index": "hash", "size": 4, "min": 1, "max": 300},  # Primary Key
            "name": {"V": 200, "index": None, "size": 20},
            "dept_name": {"V": 10, "index": None, "size": 16},
            "salary": {"V": 50, "index": None, "size": 4, "min": 50000, "max": 200000},
        }
    },
    "prereq": {
        "n": 200,
        "attributes": {
            "course_id": {"V": 500, "index": "hash", "size": 8},  # Primary Key
            "prereq_id": {"V": 200, "index": "hash", "size": 8}, # primary key
        }
    },
    "classroom": {
        "n": 50,
        "attributes": {
            "building": {"V": 10, "index": "hash", "size": 16},  # Primary Key
            "room_no": {"V": 50, "index": None, "size": 4, "min": 100, "max": 999},  # Composite Primary Key
            "capacity": {"V": 20, "index": None, "size": 2, "min": 10, "max": 500},
        }
    },
    "section": {
        "n": 600,
        "attributes": {
            "course_id": {"V": 500, "index": "hash", "size": 8},  # Primary Key
            "sec_id": {"V": 50, "index": "hash", "size": 2, "min": 1, "max": 50},  # Primary Key
            "semester": {"V": 4, "index": "hash", "size": 1, "min": 1, "max": 4}, # Primary Key
            "year": {"V": 10, "index": "hash", "size": 2, "min": 2020, "max": 2030}, # Primary Key
            "building": {"V": 10, "index": None, "size": 16},
            "room_no": {"V": 50, "index": None, "size": 4, "min": 100, "max": 999},
            "time_slot_id": {"V": 20, "index": None, "size": 2, "min": 1, "max": 20},
        }
    },
    "teaches": {
        "n": 1200,
        "attributes": {
            "id": {"V": 300, "index": "hash", "size": 4, "min": 1, "max": 300},  # Primary Key
            "course_id": {"V": 500, "index": "hash", "size": 8}, # Primary Key
            "sec_id": {"V": 50, "index": "hash", "size": 2, "min": 1, "max": 50}, # Primary Key
            "semester": {"V": 4, "index": "hash", "size": 1, "min": 1, "max": 4}, # Primary Key
            "year": {"V": 10, "index": "hash", "size": 2, "min": 2020, "max": 2030}, # Primary Key
        }
    },
    "time_slot": {
        "n": 50,
        "attributes": {
            "time_slot_id": {"V": 50, "index": "hash", "size": 2, "min": 1, "max": 50},  # Primary Key
            "day": {"V": 7, "index": "hash", "size": 1, "min": 1, "max": 7}, # Primary Key
            "start_time": {"V": 30, "index": "hash", "size": 2, "min": 800, "max": 2000}, # Primary Key
            "end_time": {"V": 30, "index": None, "size": 2, "min": 850, "max": 2050},
        }
    },
    "department": {
        "n": 10,
        "attributes": {
            "dept_name": {"V": 10, "index": "hash", "size": 16},  # Primary Key
            "building": {"V": 5, "index": None, "size": 16},
            "budget": {"V": 10, "index": None, "size": 8, "min": 100000, "max": 10000000},
        }
    },
}

class QOData:
    '''
    Singleton class for managing query optimization data
    '''
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(QOData, cls).__new__(cls)
            cls._instance.data = MOCK_DATA  # Initialize the data attribute
        return cls._instance

    def __init__(self):
        """
        Initialize method is empty since initialization is handled in __new__
        This prevents re-initialization when getting the instance multiple times
        """
        pass
    
    @classmethod
    def get_instance(cls) -> 'QOData':
        """Get the singleton instance of QOData"""
        return cls()

    def get_n(self, relation: str) -> int:
        """Get the number of tuples in the relation."""
        ret = self.data.get(relation, {}).get("n", 0)
        if (not ret):
            raise ValueError("Relation not found")
        return ret

    def get_index(self, attribute: str, relation: str) -> Union[Literal["hash", "btree"], None]:
        """Get the type of index on the given attribute in the relation."""
        return self.data.get(relation, {}).get("attributes", {}).get(attribute, {}).get("index", None)
    
    def has_index(self, attribute: str, relation: str) -> bool:
        """Check if the attribute in the relation has an index."""
        return self.get_index(attribute, relation) is not None

    def get_V(self, attribute: str, relation: str) -> int:
        """Get the number of distinct values for the attribute in the relation."""
        ret = self.data.get(relation, {}).get("attributes", {}).get(attribute, {}).get("V", 0)
        if (not ret):
            raise ValueError("Relation or attribute not found")
        return ret

    def get_b(self, relation: str) -> int:
        """
        Get the number of blocks in the relation.
        Args:
            relation (str): Name of the relation
            
        Returns:
            int: Number of blocks needed for the relation
            
        Raises:
            ValueError: If relation not found
        """
        if relation not in self.data:
            raise ValueError("Relation not found")
            
        # Calculate record size
        record_size = sum(attr['size'] for attr in self.data[relation]['attributes'].values())
        
        # Calculate blocking factor (records per block)
        bfr = self.BLOCK_SIZE // record_size
        
        # Calculate number of blocks needed (ceiling division)
        num_tuples = self.data[relation]['n']
        num_blocks = (num_tuples + bfr - 1) // bfr
        
        return num_blocks

    def get_f(self, relation: str) -> int:
        """
        Get the number of tuples per block in the relation.
        
        Args:
            relation (str): Name of the relation
            
        Returns:
            int: Number of tuples that fit in one block
            
        Raises:
            ValueError: If relation not found
        """
        if relation not in self.data:
            raise ValueError("Relation not found")
            
        # Calculate record size
        record_size = sum(attr['size'] for attr in self.data[relation]['attributes'].values())
        
        # Calculate and return blocking factor
        return self.BLOCK_SIZE // record_size
    
    def get_min(self, attribute: str, relation: str) -> Union[int, None]:
        """
        Get the minimum value for an attribute in a relation.
        
        Args:
            attribute (str): Name of the attribute
            relation (str): Name of the relation
            
        Returns:
            Union[int, None]: Minimum value if it exists, None for string attributes
            
        Raises:
            KeyError: If relation or attribute not found
        """
        if relation not in self.data:
            raise KeyError(f"Relation {relation} not found")
            
        if attribute not in self.data[relation]['attributes']:
            raise KeyError(f"Attribute {attribute} not found in relation {relation}")
            
        return self.data[relation]['attributes'][attribute].get('min')

    def get_max(self, attribute: str, relation: str) -> Union[int, None]:
        """
        Get the maximum value for an attribute in a relation.
        
        Args:
            attribute (str): Name of the attribute
            relation (str): Name of the relation
            
        Returns:
            Union[int, None]: Maximum value if it exists, None for string attributes
            
        Raises:
            KeyError: If relation or attribute not found
        """
        if relation not in self.data:
            raise KeyError(f"Relation {relation} not found")
            
        if attribute not in self.data[relation]['attributes']:
            raise KeyError(f"Attribute {attribute} not found in relation {relation}")
            
        return self.data[relation]['attributes'][attribute].get('max')

    def get_all_relations(self):
        """Get all relations in the data."""
        return list(self.data.keys())

    def get_all_attributes(self, relation: str):
        """Get all attributes in the relation."""
        return list(self.data.get(relation, {}).get("attributes", {}).keys())
    
    def has_relation(self, relation: str) -> bool:
        """Check if the relation is in the data."""
        return relation in self.data
    
    def has_attribute(self, attribute: str, relation: str) -> bool:
        """Check if the attribute is in the relation."""
        return attribute in self.get_all_attributes(relation)
    
    def get_size(self, attribute: str, relation: str) -> int:
        """
        Get the size in bytes for an attribute in a relation.
        
        Args:
            attribute (str): Name of the attribute
            relation (str): Name of the relation
            
        Returns:
            int: Size of the attribute in bytes
            
        Raises:
            KeyError: If relation or attribute not found
        """
        if relation not in self.data:
            raise KeyError(f"Relation {relation} not found")
            
        if attribute not in self.data[relation]['attributes']:
            raise KeyError(f"Attribute {attribute} not found in relation {relation}")
            
        return self.data[relation]['attributes'][attribute]['size']
    


def calculate_join_cost(table1_name, table2_name, join_attribute):

    def calculate_pages(table_info):
        row_size = sum(attr["size"] for attr in table_info["attributes"].values())
        rows_per_page = BLOCK_SIZE // row_size
        return max(1, table_info["n"] // rows_per_page)

    table1 = MOCK_DATA[table1_name]
    table2 = MOCK_DATA[table2_name]
    
    outer_pages = calculate_pages(table1)
    inner_pages = calculate_pages(table2)
    outer_rows = table1["n"]
    inner_rows = table2["n"]

    has_index = table2["attributes"].get(join_attribute, {}).get("index") is not None

    costs = {
        "nested_loop_join": {
            "cost": outer_pages + (outer_rows * inner_pages),
            "formula": f"Cost = {outer_pages} + ({outer_rows} × {inner_pages})",
            "description": "Simple nested loop join"
        }
    }

    if has_index:
        index_cost = outer_pages + (outer_rows * (1 + max(1, round(math.log2(inner_pages)))))
        costs["index_nested_loop_join"] = {
            "cost": index_cost,
            "formula": f"Cost = {outer_pages} + ({outer_rows} × (1 + log₂({inner_pages})))",
            "description": "Index nested loop join using hash index"
        }

    buffer_pages = 10  # Assuming we can hold 10 pages in memory
    block_cost = outer_pages + (math.ceil(outer_pages/buffer_pages) * inner_pages)
    costs["block_nested_loop_join"] = {
        "cost": block_cost,
        "formula": f"Cost = {outer_pages} + ⌈{outer_pages}/{buffer_pages}⌉ × {inner_pages}",
        "description": f"Block nested loop join with buffer size of {buffer_pages} pages"
    }

    # Add table statistics to the output
    costs["statistics"] = {
        "block_size": BLOCK_SIZE,
        f"{table1_name}_pages": outer_pages,
        f"{table1_name}_rows": outer_rows,
        f"{table2_name}_pages": inner_pages,
        f"{table2_name}_rows": inner_rows,
        "has_index": has_index,
        f"{table1_name}_row_size": sum(attr["size"] for attr in table1["attributes"].values()),
        f"{table2_name}_row_size": sum(attr["size"] for attr in table2["attributes"].values())
    }

    return costs