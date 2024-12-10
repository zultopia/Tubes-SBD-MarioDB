from typing import Literal, Union

MOCK_DATA = {
    "student": {
        "n": 1000,
        "attributes": {
            "id": {"V": 1000, "index": "hash"},  # Primary Key
            "name": {"V": 800, "index": None},
            "dept_name": {"V": 10, "index": None},
            "tot_cred": {"V": 50, "index": None},
        },
        "b": 100,
        "l": 10.0,
        "f": 10,
    },
    "advisor": {
        "n": 500,
        "attributes": {
            "s_id": {"V": 1000, "index": "hash"},  # Primary Key
            "i_id": {"V": 500, "index": None},
        },
        "b": 50,
        "l": 8.0,
        "f": 10,
    },
    "course": {
        "n": 500,
        "attributes": {
            "course_id": {"V": 500, "index": "hash"},  # Primary Key
            "title": {"V": 400, "index": None},
            "dept_name": {"V": 10, "index": None},
            "credits": {"V": 5, "index": None},
        },
        "b": 50,
        "l": 8.0,
        "f": 10,
    },
    "takes": {
        "n": 2000,
        "attributes": {
            "id": {"V": 1000, "index": "hash"},  # Primary Key
            "course_id": {"V": 500, "index": "hash"},  # Primary Key
            "sec_id": {"V": 50, "index": "hash"},  # Primary Key
            "semester": {"V": 4, "index": "hash"},
            "year": {"V": 10, "index": "hash"},
            "grade": {"V": 5, "index": None},
        },
        "b": 200,
        "l": 10.0,
        "f": 20,
    },
    "instructor": {
        "n": 300,
        "attributes": {
            "id": {"V": 300, "index": "hash"},  # Primary Key
            "name": {"V": 200, "index": None},
            "dept_name": {"V": 10, "index": None},
            "salary": {"V": 50, "index": None},
        },
        "b": 30,
        "l": 5.0,
        "f": 5,
    },
    "prereq": {
        "n": 200,
        "attributes": {
            "course_id": {"V": 500, "index": "hash"},  # Primary Key
            "prereq_id": {"V": 200, "index": "hash"}, # primary key
        },
        "b": 20,
        "l": 10.0,
        "f": 10,
    },
    "classroom": {
        "n": 50,
        "attributes": {
            "building": {"V": 10, "index": "hash"},  # Primary Key
            "room_no": {"V": 50, "index": None},  # Composite Primary Key
            "capacity": {"V": 20, "index": None},
        },
        "b": 5,
        "l": 10.0,
        "f": 5,
    },
    "section": {
        "n": 600,
        "attributes": {
            "course_id": {"V": 500, "index": "hash"},  # Primary Key
            "sec_id": {"V": 50, "index": "hash"},  # Primary Key
            "semester": {"V": 4, "index": "hash"}, # Primary Key
            "year": {"V": 10, "index": "hash"}, # Primary Key
            "building": {"V": 10, "index": None},
            "room_no": {"V": 50, "index": None},
            "time_slot_id": {"V": 20, "index": None},
        },
        "b": 60,
        "l": 10.0,
        "f": 10,
    },
    "teaches": {
        "n": 1200,
        "attributes": {
            "id": {"V": 300, "index": "hash"},  # Primary Key
            "course_id": {"V": 500, "index": "hash"}, # Primary Key
            "sec_id": {"V": 50, "index": "hash"}, # Primary Key
            "semester": {"V": 4, "index": "hash"}, # Primary Key
            "year": {"V": 10, "index": "hash"}, # Primary Key
        },
        "b": 120,
        "l": 10.0,
        "f": 10,
    },
    "time_slot": {
        "n": 50,
        "attributes": {
            "time_slot_id": {"V": 50, "index": "hash"},  # Primary Key
            "day": {"V": 7, "index": "hash"}, # Primary Key
            "start_time": {"V": 30, "index": "hash"}, # Primary Key
            "end_time": {"V": 30, "index": None},
        },
        "b": 5,
        "l": 10.0,
        "f": 5,
    },
    "department": {
        "n": 10,
        "attributes": {
            "dept_name": {"V": 10, "index": "hash"},  # Primary Key
            "building": {"V": 5, "index": None},
            "budget": {"V": 10, "index": None},
        },
        "b": 1,
        "l": 10.0,
        "f": 1,
    },
}

class QOData:
    '''
    Still a mock class for now
    '''

    def __init__(self) -> None:
        self.data = MOCK_DATA

    def get_n(self, relation: str) -> int:
        """Get the number of tuples in the relation."""
        ret =  self.data.get(relation, {}).get("n", 0)
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
        """Get the number of blocks in the relation."""
        ret = self.data.get(relation, {}).get("b", 0)
        if (not ret):
            raise ValueError("Relation not found")
        return ret

    def get_l(self, relation: str) -> float:
        """Get the blocking factor of the relation."""
        ret = self.data.get(relation, {}).get("l", 0.0)
        if (not ret):
            raise ValueError("Relation not found")
        return ret

    def get_f(self, relation: str) -> int:
        """Get the number of tuples per block in the relation."""
        ret = self.data.get(relation, {}).get("f", 0)
        if (not ret):
            raise ValueError("Relation not found")
        return ret
